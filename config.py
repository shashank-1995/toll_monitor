from dataclasses import dataclass
import sys
from pathlib import Path
import os


@dataclass(frozen=True)
class Paths:
    base_dir: Path
    input_dir: Path
    output_dir: Path
    work_dir: Path


@dataclass(frozen=True)
class DbConfig:
    host: str
    user: str
    password: str
    database: str
    port: int


@dataclass(frozen=True)
class AppConfig:
    paths: Paths
    db: DbConfig
    csv_encoding: str
    constants_url: str | None
    constants_cache_path: Path
    license_path: Path


def _env_path(name: str, default: Path) -> Path:
    val = os.getenv(name)
    return Path(val).expanduser().resolve() if val else default


def _runtime_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = _runtime_base_dir()


PATHS = Paths(
    base_dir=BASE_DIR,
    work_dir=_env_path("TOLL_AUDIT_WORK_DIR", BASE_DIR),
    input_dir=_env_path("TOLL_AUDIT_INPUT_DIR", BASE_DIR / "Files"),
    output_dir=_env_path("TOLL_AUDIT_OUTPUT_DIR", BASE_DIR / "Files2"),
)

DB = DbConfig(
    host=os.getenv("TOLL_AUDIT_DB_HOST", "127.0.0.1"),
    user=os.getenv("TOLL_AUDIT_DB_USER", "root"),
    password=os.getenv("TOLL_AUDIT_DB_PASSWORD", ""),
    database=os.getenv("TOLL_AUDIT_DB_NAME", "tst"),
    port=int(os.getenv("TOLL_AUDIT_DB_PORT", "3306")),
)

CONFIG = AppConfig(
    paths=PATHS,
    db=DB,
    csv_encoding=os.getenv("TOLL_AUDIT_CSV_ENCODING", "latin-1"),
    constants_url=os.getenv(
        "TOLL_AUDIT_CONSTANTS_URL",
        "https://gist.githubusercontent.com/accnt4fleet-git/d5d1dbf21442727cabc08d65357f16b1/raw/51b0f1c1bba828cd3864419c539dac3ffb4e3a7a/constants.json",
    ),
    constants_cache_path=_env_path(
        "TOLL_AUDIT_CONSTANTS_CACHE", PATHS.work_dir / "constants_cache.json"),
    license_path=_env_path("TOLL_AUDIT_LICENSE_PATH",
                           BASE_DIR / "license.json"),
)
