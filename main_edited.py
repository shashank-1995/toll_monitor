import mysql.connector
import csv
import shutil
from datetime import datetime,timedelta
import traceback
import dbInfo
import  pandas as pd
from rough7 import paired_toll_error,dd_error,rj_error
import os
global source,destination1,destination2,filename
# destination1 is the address of the folder with main script
destination1= r"D:\toll-tracker-corporate-main\srce"
# destination2 is the address of the output folder
destination2= r"D:\toll-tracker-corporate-main\Files2"
# source is the address of the folder where user will put files to be processed
source= r"D:\toll-tracker-corporate-main\Files"
def mainn():
    global source,destination1,destination2,filename
# tm is date till when user can continue to use the script. After that he will have to download the script from a link
    tm="01-12-2026 00:00:01"
    dt= datetime.strptime(tm,"%d-%m-%Y %H:%M:%S")
    if datetime.now()<dt:
        print("There will be 1 or 2 disputes files in the output folder")
        empty_tablr()
        emptyy_folder()
        master()
    else:
        print("Your subscription has expired . Ask for renewal.")
def master():
    global source,destination1,destination2

    lstt= os.listdir(source)
    if len(lstt)==0:
        print("No file is there to be processed. Plz add the files")
        exit()
    else:
        for filename in lstt:
            try:
                    uploadd_data(filename)

                    Da_dispuutes(filename)
                    paired_toll_error()
                    dd_error()
                    rj_error()

                    create_despute_report(filename)
                    empty_tablr()
            except:
                traceback.print_exc()

def uploadd_data(filename):
    global source,destination1,destination2
    try:
        db = mysql.connector.connect(
            host=dbInfo.host,
            user=dbInfo.user,
            password=dbInfo.password,
            database=dbInfo.database
        )
        csv_data = csv.reader(open(source+"\\"+filename))
        next(csv_data)
        cursor = db.cursor(buffered=True)

        count = 0
        process_count = 0
        dt_string= datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
        for row in csv_data:
                if "TRIP (RRN NO / TRIP NO)" in row[5]:

                    unique_id1 = row[8].strip("ÿ").strip('Â')
                    # print(unique_id1)
                    plazacode = float(row[6].strip('Â').strip('ÿ').strip('˜'))
                    s = row[10].replace(',', '')
                    price = float(s)
                    if "Plaza Name:" in row[7]:

                        plazaname = row[7].split("Plaza Name:")[1].split('- Lane')[0]
                    else:
                        plazaname = row[7].split('- Lane')[0]

                    rrn = unique_id1.split("/")[0].strip('˜').strip()
                    trip_id = unique_id1.split("/")[1].strip()
                    dtm1 = row[0].strip()
                    # print(dtm1)
                    # dtm = datetime.strptime(dtm1, "%d/%m/%Y %H:%M:%S")
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
                    sql = "INSERT INTO t_statement (txn_dtm, lic_no, tag_no, plaza_code, plaza_name, rrn, trip_no, deduct_price,created_at,status) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (
                        dtm, row[2].strip(), row[3].strip(), plazacode, plazaname, rrn, trip_id, price,dt_string ,
                        "not processed")
                    try:
                        # Execute the SQL command
                        process_count += 1

                        cursor.execute(sql)
                        count += cursor.rowcount
                        # print(count)

                        # Commit your changes in the database

                    except:
                        # print("Error :",process_count, sys.exc_info()[0])
                        # Rollback in case there is any error
                        db.rollback()

        db.commit()

        print("Total Processed Data {} and Total {} data inserted successfully! ".format(process_count, count))

    except:
        traceback.print_exc()



def Da_dispuutes(filename):

    try:
        global source,destination1,destination2
        db = mysql.connector.connect(
            host=dbInfo.host,
            user=dbInfo.user,
            password=dbInfo.password,
            database=dbInfo.database
        )
        cursor = db.cursor(buffered=True)

        lic_no = ''
        rn = ''
        trip_no = ''

        fields = ['Type', 'Subtype', 'Trip Number', 'Dispute Amount', 'Title', 'Description']

        file2 = filename.split(".csv")[0] + 'DA_Disp_5' + '.csv'

        f = open(file2, 'w', encoding='UTF8', newline='')
        # VRN stands for vehicle registration number

        # create the csv writer
        csvwriter = csv.writer(f)

        # writing the fields
        csvwriter.writerow(fields)

        c = 0
        csv_data = csv.reader(open(source+"\\"+ filename, newline=''))
        next(csv_data)
        for row in csv_data:
            lst = []

            if "Chargeback Debit Adjustment" in row[7]:
                s = row[10].replace(',', '')
                amount = float(s)
                rn = row[7].split("RRN ")[1].strip()
                # print(rn)
                sql = "SELECT trip_no,lic_no from t_statement where rrn like '{}'".format(rn)
                cursor.execute(sql)
                res = cursor.fetchall()
                if res:
                    c += 1
                    for r in res:
                        trip_no = r[0]
                        lic_no = r[1]

                    lst.append(dbInfo.type)
                    lst.append('Wrong Debit Adjustment raised')

                    lst.append(trip_no)
                    lst.append(amount)
                    lst.append('WRONG DEBIT ADJUSTMENT')
                    lst.append('Toll operator made wrong debit. RRN is : ' + rn + ' for vehicle ,' + lic_no)
                    csvwriter.writerow(lst)

                    f.flush()

                else:
                    pass

        f.close()

        if c == 0:

            os.remove(destination1+"\\"+ file2)
        else:

            df = pd.read_csv(destination1+"\\" + file2)
            summ = df['Dispute Amount'].sum()
            print(summ, 'Disputed amount for total DA disputes for file ',filename)
            nm = file2.split('.csv')[0] + "_" + str(summ) + ".csv"
            os.rename(destination1+"\\"+ file2, destination1+"\\" + nm)
            shutil.move(destination1+"\\" + nm, destination2+"\\" + nm)

    except:
        traceback.print_exc()






def empty_tablr():
    db = mysql.connector.connect(
        host=dbInfo.host,
        user=dbInfo.user,
        password=dbInfo.password,
        database=dbInfo.database
    )

    cursor = db.cursor(buffered=True)
    cursor3 = db.cursor(buffered=True)
    try:
        qry = "TRUNCATE TABLE t_statement"
        cursor.execute(qry)
        db.commit()

        qry3 = "TRUNCATE TABLE toll_d"
        cursor3.execute(qry3)
        db.commit()
        print('2 tables emptied')
    except:
        traceback.print_exc()
def create_despute_report(filename):
    global source,destination1,destination2

    try:
        db = mysql.connector.connect(
            host=dbInfo.host,
            user=dbInfo.user,
            password=dbInfo.password,
            database=dbInfo.database
        )

        cursor = db.cursor(buffered=True)
        cur = db.cursor(buffered=True)
        fields = ['Type', 'Subtype', 'Trip Number', 'Dispute Amount', 'Title', 'Description']
        csv_file = filename.split('.csv')[0] + "_errors_upload.csv"
        # print(csv_file)

        f = open(csv_file, 'w', encoding='UTF8', newline='')

        # create the csv writer
        csvwriter = csv.writer(f)

        # writing the fields
        csvwriter.writerow(fields)
        rec_count = 0
        # data rows of csv file
        qry = "SELECT distinct tripno from toll_d"
        # print(qry)
        cursor.execute(qry)
        result = cursor.fetchall()
        if result:

            lst = []
            for r in result:
                lst.append(r[0])

            for l in lst:
                rows = []

                q = "SELECT * from toll_d where tripno='{}'".format(
                    l)
                cur.execute(q)
                res = cur.fetchall()

                rows.append(res[0][1])
                rows.append(res[0][2])

                rows.append(res[0][5])
                rows.append(res[0][6])
                rows.append(res[0][7])
                rows.append(res[0][8])

                csvwriter.writerow(rows)
                f.flush()

            f.close()

            df = pd.read_csv(destination1+"\\" + csv_file)
            summ = df['Dispute Amount'].sum()
            print(summ, 'Disputed amount for toll disputes for file ',filename)

            nm = csv_file.split('.csv')[0] + "_" + str(summ) + ".csv"
            os.rename(destination1+"\\" + csv_file, destination1+"\\"  + nm)
            shutil.move(destination1+"\\"  + nm, destination2 + "\\" + nm)

        else:
            f.close()
            os.remove(destination1+"\\" + csv_file)
    except:
        traceback.print_exc()
def emptyy_folder():
    if os.path.exists(destination2):
        for files in os.listdir(destination2):
            os.remove(os.path.join(destination2, files))

if __name__=="__main__":
    mainn()
