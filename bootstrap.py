from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable

import mysql.connector

from config import CONFIG


REQUIRED_TABLES = ("t_statement", "toll_d")


def setup_logger() -> logging.Logger:
    """
    Creates a rotating file logger + console logger.
    Logs are written inside CONFIG.paths.work_dir/logs/app.log
    """
    log_dir = CONFIG.paths.work_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("toll_audit")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    file_handler = RotatingFileHandler(
        filename=str(log_dir / "app.log"),
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def ensure_dirs(logger: logging.Logger) -> None:
    """
    Creates required directories if missing and validates they are writable.
    """
    for name, p in (
        ("work_dir", CONFIG.paths.work_dir),
        ("input_dir", CONFIG.paths.input_dir),
        ("output_dir", CONFIG.paths.output_dir),
    ):
        try:
            p.mkdir(parents=True, exist_ok=True)
            _assert_writable_dir(p)
            logger.info("OK: %s exists and is writable: %s", name, p)
        except Exception as e:
            logger.error("FAILED: %s directory check: %s (%s)", name, p, e)
            raise


def _assert_writable_dir(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        raise RuntimeError(f"Not a directory: {path}")

    test_file = path / ".write_test"
    try:
        test_file.write_text("ok", encoding="utf-8")
    finally:
        if test_file.exists():
            test_file.unlink()


def check_db(logger: logging.Logger) -> None:
    """
    Checks DB connectivity and that required tables exist.
    """
    try:
        db = mysql.connector.connect(
            host=CONFIG.db.host,
            user=CONFIG.db.user,
            password=CONFIG.db.password,
            database=CONFIG.db.database,
            port=CONFIG.db.port,
        )
        logger.info("OK: Connected to DB %s at %s:%s", CONFIG.db.database, CONFIG.db.host, CONFIG.db.port)
    except Exception as e:
        logger.error("FAILED: DB connection error (%s)", e)
        raise

    try:
        cursor = db.cursor()
        cursor.execute("SHOW TABLES;")
        tables = {row[0] for row in cursor.fetchall()}
        missing = [t for t in REQUIRED_TABLES if t not in tables]
        if missing:
            raise RuntimeError(f"Missing tables: {missing}. Run schema SQL init.")
        logger.info("OK: Required tables exist: %s", ", ".join(REQUIRED_TABLES))
    finally:
        db.close()


def check_input_files(logger: logging.Logger) -> list[Path]:
    """
    Returns a list of CSV files from input directory.
    """
    files = sorted([p for p in CONFIG.paths.input_dir.iterdir() if p.is_file()])
    if not files:
        logger.info("No input files found in: %s", CONFIG.paths.input_dir)
        return []
    logger.info("Found %d input files in %s", len(files), CONFIG.paths.input_dir)
    return files


def preflight_or_exit() -> logging.Logger:
    """
    Runs all startup checks. If anything fails, prints a friendly message and raises.
    """
    logger = setup_logger()
    logger.info("Starting Toll Audit - preflight checks")

    ensure_dirs(logger)
    check_db(logger)

    logger.info("Preflight checks completed successfully")
    return logger
