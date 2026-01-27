import csv
import logging
import os
import shutil
import time
import traceback
from datetime import datetime

import pandas as pd

import dbInfo
from bootstrap import preflight_or_exit
from config import CONFIG
from license_manager import LicenseError, get_device_id, validate_license_or_exit
from rough7 import dd_error, paired_toll_error, rj_error


CSV_ENCODING = CONFIG.csv_encoding

PATHS = {
    "destination1": str(CONFIG.paths.base_dir),
    "destination2": str(CONFIG.paths.output_dir),
    "source": str(CONFIG.paths.input_dir),
}

LOGGER = logging.getLogger("toll_audit")


def _ensure_dirs() -> None:
    os.makedirs(PATHS["destination1"], exist_ok=True)
    os.makedirs(PATHS["destination2"], exist_ok=True)
    os.makedirs(PATHS["source"], exist_ok=True)


def _file_path(*parts: str) -> str:
    return os.path.join(*parts)


def _fmt_seconds(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = seconds / 60.0
    return f"{minutes:.2f}m"


def mainn() -> None:
    global LOGGER
    LOGGER = preflight_or_exit()

    LOGGER.info("Runtime paths:")
    LOGGER.info("  base_dir: %s", PATHS["destination1"])
    LOGGER.info("  input_dir: %s", PATHS["source"])
    LOGGER.info("  output_dir: %s", PATHS["destination2"])
    LOGGER.info("CSV encoding: %s", CSV_ENCODING)

    device_id = get_device_id()
    LOGGER.info("Device ID: %s", device_id)

    try:
        validate_license_or_exit(expected_device_id=device_id)
        LOGGER.info("License OK")
    except LicenseError as exc:
        LOGGER.exception("License error: %s", exc)
        print("License error:", str(exc))
        print("Your Device ID:", device_id)
        raise SystemExit(1)
    except SystemExit as exc:
        LOGGER.exception("License validation failed: %s", exc)
        print(str(exc))
        print("Your Device ID:", device_id)
        raise

    _ensure_dirs()

    print("There will be 1 or 2 disputes files in the output folder")
    LOGGER.info("Starting run pipeline")

    empty_tablr()
    emptyy_folder()
    master()

    LOGGER.info("Run pipeline finished")


def master() -> None:
    source = PATHS["source"]

    try:
        lstt = os.listdir(source)
    except Exception:
        LOGGER.exception("Failed to list input directory: %s", source)
        raise

    LOGGER.info("Input dir scan: %s", source)
    LOGGER.info("Found %d file(s): %s", len(lstt), lstt)

    if len(lstt) == 0:
        print("No file is there to be processed. Plz add the files")
        LOGGER.warning("No file found in input directory; exiting.")
        raise SystemExit(0)

    for filename in lstt:
        file_path = _file_path(source, filename)
        try:
            size_bytes = os.path.getsize(file_path)
        except Exception:
            size_bytes = -1

        LOGGER.info("-----")
        LOGGER.info("Processing file: %s (size=%s bytes)", filename, size_bytes)
        LOGGER.info("Full path: %s", file_path)

        try:
            t0 = time.perf_counter()

            LOGGER.info("Step: uploadd_data START")
            uploadd_data(filename)
            LOGGER.info("Step: uploadd_data END (%s)", _fmt_seconds(time.perf_counter() - t0))

            t1 = time.perf_counter()
            LOGGER.info("Step: Da_dispuutes START")
            Da_dispuutes(filename)
            LOGGER.info("Step: Da_dispuutes END (%s)", _fmt_seconds(time.perf_counter() - t1))

            # Existing dispute logic (unchanged)
            t2 = time.perf_counter()
            LOGGER.info("Step: paired_toll_error START")
            paired_toll_error()
            LOGGER.info("Step: paired_toll_error END (%s)", _fmt_seconds(time.perf_counter() - t2))

            t3 = time.perf_counter()
            LOGGER.info("Step: dd_error START")
            dd_error()
            LOGGER.info("Step: dd_error END (%s)", _fmt_seconds(time.perf_counter() - t3))

            t4 = time.perf_counter()
            LOGGER.info("Step: rj_error START")
            rj_error()
            LOGGER.info("Step: rj_error END (%s)", _fmt_seconds(time.perf_counter() - t4))

            create_despute_report(filename)

            empty_tablr()

            LOGGER.info(
                "Completed file: %s (total=%s)",
                filename,
                _fmt_seconds(time.perf_counter() - t0),
            )
        except Exception:
            LOGGER.exception("Failed while processing file: %s", filename)
            traceback.print_exc()


def uploadd_data(filename: str) -> None:
    source = PATHS["source"]
    input_path = _file_path(source, filename)

    LOGGER.info("uploadd_data: reading CSV: %s", input_path)

    try:
        db = dbInfo.get_connection()
        cursor = db.cursor(buffered=True)

        t_start = time.perf_counter()
        dt_string = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

        count = 0
        process_count = 0
        rows_seen = 0
        matched_rows = 0

        with open(input_path, newline="", encoding=CSV_ENCODING, errors="replace") as fh:
            csv_data = csv.reader(fh)
            next(csv_data, None)

            for row in csv_data:
                rows_seen += 1

                # Progress log every 50k rows (tune if you want)
                if rows_seen % 50000 == 0:
                    elapsed = time.perf_counter() - t_start
                    LOGGER.info(
                        "uploadd_data progress: rows_seen=%d matched=%d inserted=%d elapsed=%s",
                        rows_seen,
                        matched_rows,
                        count,
                        _fmt_seconds(elapsed),
                    )

                if len(row) < 11:
                    continue

                if "TRIP (RRN NO / TRIP NO)" not in row[5]:
                    continue

                matched_rows += 1

                unique_id1 = row[8].strip("ÿ").strip("Â")
                plazacode = float(row[6].strip("Â").strip("ÿ").strip("˜"))
                price = float(row[10].replace(",", ""))

                if "Plaza Name:" in row[7]:
                    plazaname = row[7].split("Plaza Name:")[1].split("- Lane")[0]
                else:
                    plazaname = row[7].split("- Lane")[0]

                rrn = unique_id1.split("/")[0].strip("˜").strip()
                trip_id = unique_id1.split("/")[1].strip()

                dtm1 = row[0].strip()
                if len(dtm1) >= 18:
                    if "/" in dtm1:
                        dtm = datetime.strptime(dtm1, "%d/%m/%Y %H:%M:%S")
                    else:
                        dtm = datetime.strptime(dtm1, "%Y-%m-%d %H:%M:%S")
                else:
                    if "/" in dtm1:
                        dtm = datetime.strptime(dtm1, "%d/%m/%y %H:%M")
                    else:
                        dtm = datetime.strptime(dtm1, "%d-%m-%Y %H:%M")

                sql = (
                    "INSERT INTO t_statement "
                    "(txn_dtm, lic_no, tag_no, plaza_code, plaza_name, rrn, trip_no, deduct_price, created_at, status) "
                    "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
                    % (
                        dtm,
                        row[2].strip(),
                        row[3].strip(),
                        plazacode,
                        plazaname,
                        rrn,
                        trip_id,
                        price,
                        dt_string,
                        "not processed",
                    )
                )

                try:
                    process_count += 1
                    cursor.execute(sql)
                    count += cursor.rowcount
                except Exception:
                    db.rollback()

        db.commit()

        elapsed_total = time.perf_counter() - t_start
        LOGGER.info(
            "uploadd_data DONE: rows_seen=%d matched=%d processed=%d inserted=%d elapsed=%s",
            rows_seen,
            matched_rows,
            process_count,
            count,
            _fmt_seconds(elapsed_total),
        )

        print(f"Total Processed Data {process_count} and Total {count} data inserted successfully! ")

    except Exception:
        LOGGER.exception("uploadd_data failed for file: %s", filename)
        traceback.print_exc()


def Da_dispuutes(filename: str) -> None:
    source = PATHS["source"]
    destination1 = PATHS["destination1"]
    destination2 = PATHS["destination2"]

    input_path = _file_path(source, filename)

    LOGGER.info("Da_dispuutes: scanning file: %s", input_path)

    try:
        db = dbInfo.get_connection()
        cursor = db.cursor(buffered=True)

        fields = ["Type", "Subtype", "Trip Number", "Dispute Amount", "Title", "Description"]

        file2_name = filename.split(".csv")[0] + "DA_Disp_5" + ".csv"
        file2_path = _file_path(destination1, file2_name)

        c = 0
        t_start = time.perf_counter()

        with open(file2_path, "w", encoding="utf-8", newline="") as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(fields)

            rows_seen = 0
            with open(input_path, newline="", encoding=CSV_ENCODING, errors="replace") as fh:
                csv_data = csv.reader(fh)
                next(csv_data, None)

                for row in csv_data:
                    rows_seen += 1
                    if rows_seen % 50000 == 0:
                        LOGGER.info("Da_dispuutes progress: rows_seen=%d disputes_found=%d", rows_seen, c)

                    if len(row) < 11:
                        continue

                    if "Chargeback Debit Adjustment" not in row[7]:
                        continue

                    amount = float(row[10].replace(",", ""))
                    rn = row[7].split("RRN ")[1].strip()

                    sql = "SELECT trip_no, lic_no from t_statement where rrn like '{}'".format(rn)
                    cursor.execute(sql)
                    res = cursor.fetchall()

                    if not res:
                        continue

                    c += 1
                    trip_no, lic_no = res[0][0], res[0][1]

                    csvwriter.writerow(
                        [
                            dbInfo.type,
                            "Wrong Debit Adjustment raised",
                            trip_no,
                            amount,
                            "WRONG DEBIT ADJUSTMENT",
                            "Toll operator made wrong debit. RRN is : " + rn + " for vehicle ," + lic_no,
                        ]
                    )
                    f.flush()

        if c == 0:
            try:
                os.remove(file2_path)
            except Exception:
                LOGGER.exception("Failed to remove empty DA dispute file: %s", file2_path)

            LOGGER.info("Da_dispuutes DONE: no disputes found (removed output file). elapsed=%s", _fmt_seconds(time.perf_counter() - t_start))
            return

        df = pd.read_csv(file2_path)
        summ = df["Dispute Amount"].sum()
        print(summ, "Disputed amount for total DA disputes for file ", filename)

        nm = file2_name.split(".csv")[0] + "_" + str(summ) + ".csv"
        nm_path = _file_path(destination1, nm)

        os.rename(file2_path, nm_path)
        shutil.move(nm_path, _file_path(destination2, nm))

        LOGGER.info(
            "Da_dispuutes DONE: disputes_found=%d total_amount=%s output=%s elapsed=%s",
            c,
            summ,
            _file_path(destination2, nm),
            _fmt_seconds(time.perf_counter() - t_start),
        )

    except Exception:
        LOGGER.exception("Da_dispuutes failed for file: %s", filename)
        traceback.print_exc()


def empty_tablr() -> None:
    try:
        LOGGER.info("empty_tablr: truncating tables")
        db = dbInfo.get_connection()

        cursor = db.cursor(buffered=True)
        cursor3 = db.cursor(buffered=True)

        cursor.execute("TRUNCATE TABLE t_statement")
        db.commit()

        cursor3.execute("TRUNCATE TABLE toll_d")
        db.commit()

        print("2 tables emptied")
        LOGGER.info("empty_tablr: done")
    except Exception:
        LOGGER.exception("empty_tablr failed")
        traceback.print_exc()


def create_despute_report(filename: str) -> None:
    destination1 = PATHS["destination1"]
    destination2 = PATHS["destination2"]

    LOGGER.info("create_despute_report: START for %s", filename)

    try:
        db = dbInfo.get_connection()

        cursor = db.cursor(buffered=True)
        cur = db.cursor(buffered=True)

        fields = ["Type", "Subtype", "Trip Number", "Dispute Amount", "Title", "Description"]
        csv_file_name = filename.split(".csv")[0] + "_errors_upload.csv"
        csv_file_path = _file_path(destination1, csv_file_name)

        rec_count = 0
        with open(csv_file_path, "w", encoding="utf-8", newline="") as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(fields)

            cursor.execute("SELECT distinct tripno from toll_d")
            result = cursor.fetchall()

            if not result:
                LOGGER.info("create_despute_report: no rows in toll_d; returning")
                return

            tripnos = [r[0] for r in result]
            for tripno in tripnos:
                q = "SELECT * from toll_d where tripno='{}'".format(tripno)
                cur.execute(q)
                res = cur.fetchall()
                if not res:
                    continue

                r0 = res[0]
                csvwriter.writerow([r0[1], r0[2], r0[5], r0[6], r0[7], r0[8]])
                f.flush()
                rec_count += 1

        df = pd.read_csv(csv_file_path)
        summ = df["Dispute Amount"].sum()
        print(summ, "Disputed amount for toll disputes for file ", filename)

        nm = csv_file_name.split(".csv")[0] + "_" + str(summ) + ".csv"
        nm_path = _file_path(destination1, nm)

        os.rename(csv_file_path, nm_path)
        shutil.move(nm_path, _file_path(destination2, nm))

        LOGGER.info(
            "create_despute_report: DONE rows=%d total_amount=%s output=%s",
            rec_count,
            summ,
            _file_path(destination2, nm),
        )

    except Exception:
        LOGGER.exception("create_despute_report failed for file: %s", filename)
        traceback.print_exc()


def emptyy_folder() -> None:
    destination2 = PATHS["destination2"]
    LOGGER.info("emptyy_folder: clearing output folder: %s", destination2)

    if os.path.exists(destination2):
        for files in os.listdir(destination2):
            try:
                os.remove(_file_path(destination2, files))
            except Exception:
                LOGGER.exception("Failed to remove output file: %s", _file_path(destination2, files))

    LOGGER.info("emptyy_folder: done")


if __name__ == "__main__":
    mainn()

