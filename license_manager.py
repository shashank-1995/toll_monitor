import base64
import hashlib
import json
import os
import platform
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import CONFIG


PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsYgEPZ1CNex2dLDbNvsz
/IfsGygDWxZ1Iv238QUBu5hYC3CP9Z3T7VWeMTjY/dvvJyL3TrhqS9dB0ur+eqs/
fTriozwen0BluWq1QSXPy8u+pXBJ9ZoR218ZbMshEjWwDnDSuE2plqcMHA/ONqWF
Tj5hs2F8ue6GYe3gQNihWdCpyczbgwNO1FSSZrtzIJ/pvwdL+I6v18xpv5Leffor
hkxlhMPnDoBrV9u+FF2mmxTDh7T6v1HWLjS+UMGhOOd3alow8DhCA2CGVURTLEko
+XC1A7BtHh1XX91ba4ElVbspKB+ZHRiMmexXfEZ3iyotwyW4TUvdtKSf4Ewdgezr
swIDAQAB
-----END PUBLIC KEY-----
"""


class LicenseError(RuntimeError):
    pass


@dataclass(frozen=True)
class LicenseInfo:
    payload: Dict[str, Any]
    signature_b64: str
    expires_at_utc: datetime


def _canonical_payload_bytes(payload: Dict[str, Any]) -> bytes:
    """
    Must match Apps Script signing.
    If Apps Script signs JSON.stringify(payload, Object.keys(payload).sort()),
    this Python canonicalization matches for a flat dict (no nested dicts).
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _parse_iso_utc(dt_str: str) -> datetime:
    """
    Parses ISO8601 '...Z' and returns aware UTC datetime.
    """
    if not dt_str or not isinstance(dt_str, str):
        raise LicenseError("Missing or invalid 'expires_at' in license payload.")
    s = dt_str.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise LicenseError(f"Invalid ISO datetime in license payload: {dt_str}") from exc
    if dt.tzinfo is None:
        # Treat naive as UTC (shouldn't happen if you keep Z)
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_license(path: Path) -> LicenseInfo:
    if not path.exists():
        raise LicenseError(f"License file not found: {path}")

    try:
        lic = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise LicenseError(f"Failed to read license JSON: {path}") from exc

    payload = lic.get("payload")
    signature_b64 = lic.get("signature")

    if not isinstance(payload, dict):
        raise LicenseError("License JSON missing 'payload' object.")
    if not isinstance(signature_b64, str) or not signature_b64.strip():
        raise LicenseError("License JSON missing 'signature' string.")

    expires_at = _parse_iso_utc(str(payload.get("expires_at", "")))
    return LicenseInfo(payload=payload, signature_b64=signature_b64.strip(), expires_at_utc=expires_at)


def verify_signature(payload: Dict[str, Any], signature_b64: str) -> None:
    try:
        sig = base64.b64decode(signature_b64, validate=True)
    except Exception as exc:
        raise LicenseError("Signature is not valid base64.") from exc

    try:
        pub = serialization.load_pem_public_key(PUBLIC_KEY_PEM)
    except Exception as exc:
        raise LicenseError("Invalid PUBLIC_KEY_PEM. Ensure it is a PUBLIC KEY (not private).") from exc

    msg = _canonical_payload_bytes(payload)

    try:
        pub.verify(sig, msg, padding.PKCS1v15(), hashes.SHA256())
    except Exception as exc:
        raise LicenseError("License signature verification failed.") from exc


def _win_machine_guid() -> Optional[str]:
    """
    Returns Windows MachineGuid if available. Works on Windows only.
    """
    try:
        out = subprocess.check_output(
            ["reg", "query", r"HKLM\SOFTWARE\Microsoft\Cryptography", "/v", "MachineGuid"],
            stderr=subprocess.STDOUT,
            text=True,
        )
        # Output contains: MachineGuid    REG_SZ    <guid>
        parts = out.strip().split()
        return parts[-1] if parts else None
    except Exception:
        return None


def _mac_platform_uuid() -> Optional[str]:
    """
    Returns Mac hardware UUID using ioreg. Works on macOS only.
    """
    try:
        out = subprocess.check_output(
            ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in out.splitlines():
            if "IOPlatformUUID" in line:
                return line.split('"')[-2]
        return None
    except Exception:
        return None


def _linux_machine_id() -> Optional[str]:
    """
    Returns /etc/machine-id on Linux if present.
    """
    for p in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            val = Path(p).read_text(encoding="utf-8").strip()
            if val:
                return val
        except Exception:
            continue
    return None


def get_device_id() -> str:
    """
    Returns a stable device identifier.
    Use this value when requesting a license from client.
    """
    sysname = platform.system().lower()

    raw: Optional[str] = None
    if "windows" in sysname:
        raw = _win_machine_guid()
    elif "darwin" in sysname:
        raw = _mac_platform_uuid()
    else:
        raw = _linux_machine_id()

    if not raw:
        # Fallback: MAC-based node id
        raw = str(uuid.getnode())

    # Make it short and stable; the sheet can store a human-friendly version too.
    # You can also display the raw hash to user as "DEV-xxxx".
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12].upper()
    return f"DEV-{h}"


def validate_license_or_exit(expected_device_id: Optional[str] = None) -> LicenseInfo:
    """
    Validates license:
      - file exists
      - signature OK
      - device_id matches
      - not expired (expires_at is UTC moment corresponding to IST end-of-day)
    """
    lic = load_license(CONFIG.license_path)

    verify_signature(lic.payload, lic.signature_b64)

    device_id = expected_device_id or get_device_id()
    payload_device_id = str(lic.payload.get("device_id", "")).strip()

    if not payload_device_id:
        raise SystemExit("License invalid: missing device_id in payload.")
    if payload_device_id != device_id:
        raise SystemExit(f"License invalid: device_id mismatch. Expected {device_id}, got {payload_device_id}.")

    now = datetime.now(timezone.utc)
    if now > lic.expires_at_utc:
        raise SystemExit("License expired. Please contact support for renewal.")

    return lic

