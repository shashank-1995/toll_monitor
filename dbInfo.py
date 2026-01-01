from __future__ import annotations

from dataclasses import dataclass
import mysql.connector

from config import CONFIG


# ---- Dispute metadata (keep your existing values here) ----
# If you already had these in older dbInfo.py, keep the same values.
# I’m setting placeholders; replace with your real ones.
type = "Toll Dispute"
priority = "High"
severity = "Medium"


# ---- DB connection attributes (backward compatible) ----
host = CONFIG.db.host
user = CONFIG.db.user
password = CONFIG.db.password
database = CONFIG.db.database
port = CONFIG.db.port


def get_connection() -> mysql.connector.connection.MySQLConnection:
    """
    Single DB connection factory used everywhere.
    Always returns a new connection (safe for repeated runs and avoids stale globals).
    """
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
    )
