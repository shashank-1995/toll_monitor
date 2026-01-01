import csv
import os
import shutil
import traceback
from datetime import datetime
from bootstrap import preflight_or_exit
from license_manager import validate_license_or_exit, get_device_id

import pandas as pd

import dbInfo
from rough7 import paired_toll_error, dd_error, rj_error

try:
    # config.py must exist next to this script
    from config import PATHS, CSV_ENCODING
except Exception:
    # Safe fallback so script still runs if config.py is missing
    PATHS = {
        "destination1": os.path.dirname(os.path.abspath(__file__)),
        "destination2": os.path.join(os.path.dirname(os.path.abspath(__file__)), "Files2"),
        "source": os.path.join(os.path.dirname(os.path.abspath(__file__)), "Files"),
    }
    CSV_ENCODING = "latin-1"
    SUBSCRIPTION_EXPIRY = "01-12-2026 00:00:01"


def _ensure_dirs() -> None:
    os.makedirs(PATHS["destination1"], exist_ok=True)
    os.makedirs(PATHS["destination2"], exist_ok=True)
    os.makedirs(PATHS["source"], exist_ok=True)


def _file_path(*parts: str) -> str:
    return os.path.join(*parts)


def mainn() -> None:
    try:
        validate_license_or_exit()
    except SystemExit as e:
        print(str(e))
        print("Your Device ID:", get_device_id())
        raise
    _ensure_dirs()
    print("There will be 1 or 2 disputes files in the output folder")
    empty_tablr()
    emptyy_folder()
    master()


def master() -> None:
    source = PATHS["source"]
    lstt = os.listdir(source)

    if len(lstt) == 0:
        print("No file is there to be processed. Plz add the files")
        raise SystemExit(0)

    for filename in lstt:
        try:
            uploadd_data(filename)
            Da_dispuutes(filename)

            # Existing dispute logic (unchanged)
            paired_toll_error()
            dd_error()
            # rj_error()

            # create_despute_report(filename)
            empty_tablr()
        except Exception:
            traceback.print_exc()


def uploadd_data(filename: str) -> None:
    source = PATHS["source"]
    input_path = _file_path(source, filename)

    try:
        db = dbInfo.get_connection()
        cursor = db.cursor(buffered=True)

        with open(input_path, newline="", encoding=CSV_ENCODING, errors="replace") as fh:
            csv_data = csv.reader(fh)
            next(csv_data, None)

            count = 0
            process_count = 0
            dt_string = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

            for row in csv_data:
                if len(row) < 11:
                    continue

                if "TRIP (RRN NO / TRIP NO)" not in row[5]:
                    continue

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
        print(f"Total Processed Data {process_count} and Total {count} data inserted successfully! ")

    except Exception:
        traceback.print_exc()


def Da_dispuutes(filename: str) -> None:
    source = PATHS["source"]
    destination1 = PATHS["destination1"]
    destination2 = PATHS["destination2"]

    input_path = _file_path(source, filename)

    try:
        db = dbInfo.get_connection()

        cursor = db.cursor(buffered=True)

        fields = ["Type", "Subtype", "Trip Number", "Dispute Amount", "Title", "Description"]

        file2_name = filename.split(".csv")[0] + "DA_Disp_5" + ".csv"
        file2_path = _file_path(destination1, file2_name)

        c = 0
        with open(file2_path, "w", encoding="utf-8", newline="") as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(fields)

            with open(input_path, newline="", encoding=CSV_ENCODING, errors="replace") as fh:
                csv_data = csv.reader(fh)
                next(csv_data, None)

                for row in csv_data:
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
                            "Toll operator made wrong debit. RRN is : "
                            + rn
                            + " for vehicle ,"
                            + lic_no,
                        ]
                    )
                    f.flush()

        if c == 0:
            os.remove(file2_path)
            return

        df = pd.read_csv(file2_path)
        summ = df["Dispute Amount"].sum()
        print(summ, "Disputed amount for total DA disputes for file ", filename)

        nm = file2_name.split(".csv")[0] + "_" + str(summ) + ".csv"
        nm_path = _file_path(destination1, nm)

        os.rename(file2_path, nm_path)
        shutil.move(nm_path, _file_path(destination2, nm))

    except Exception:
        traceback.print_exc()


def empty_tablr() -> None:
    try:
        db = dbInfo.get_connection()

        cursor = db.cursor(buffered=True)
        cursor3 = db.cursor(buffered=True)

        cursor.execute("TRUNCATE TABLE t_statement")
        db.commit()

        cursor3.execute("TRUNCATE TABLE toll_d")
        db.commit()

        print("2 tables emptied")
    except Exception:
        traceback.print_exc()


def create_despute_report(filename: str) -> None:
    destination1 = PATHS["destination1"]
    destination2 = PATHS["destination2"]

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

    except Exception:
        traceback.print_exc()


def emptyy_folder() -> None:
    destination2 = PATHS["destination2"]
    if os.path.exists(destination2):
        for files in os.listdir(destination2):
            try:
                os.remove(_file_path(destination2, files))
            except Exception:
                traceback.print_exc()


if __name__ == "__main__":
    preflight_or_exit()
    mainn()
