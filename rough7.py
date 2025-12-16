
import mysql.connector
import dbInfo
from datetime import datetime, timedelta
import traceback

from constants2 import lst_rj,same_day,paired_tolls,dt

def rj_error():
    try:

        db = mysql.connector.connect(
            host=dbInfo.host,
            user=dbInfo.user,
            password=dbInfo.password,
            database=dbInfo.database
        )
        cursor = db.cursor(buffered=True)

        cr = db.cursor(buffered=True)
        coo = db.cursor(buffered=True)
        crs = db.cursor(buffered=True)

        qre = "SELECT distinct plaza_code from t_statement"
        crs.execute(qre)
        resw = crs.fetchall()
        lst2 = []
        for q in resw:
            lst2.append(q[0])

        qry = "SELECT distinct lic_no from t_statement"
        cursor.execute(qry)
        resultc = cursor.fetchall()
        lst = []
        for q in resultc:
            lst.append(q[0])
        rj = 0
        for hh in lst_rj:
            if hh in lst2:
                lst2.remove(hh)


        for vr in lst:
                for cd in lst2:
                    st = []
                    q = "SELECT * from t_statement where lic_no ='{}' and plaza_code ='{}' and txn_dtm >='{}' order by txn_dtm".format(
                        vr, cd, dt)
                    cr.execute(q)
                    re = cr.fetchall()
                    if re and len(re) > 1:
                        # for p in re:
                        #     st.append(p[8])
                        #
                        # nax = max(st)
                        for count, i in enumerate(re):
                            for cnt, j in enumerate(re):
                                if i[4] in same_day:
                                    if cnt - count == 1 and j[1] - timedelta(minutes=20) > i[1] and i[1].date() == j[
                                        1].date() and j[
                                        8] == i[8] and i[8] > 0:
                                        rj += 1
                                        t = dbInfo.type
                                        s = 'Return Journey Fare Calculated Incorrectly'
                                        p = dbInfo.priority
                                        se = dbInfo.severity
                                        tr2 = j[7]
                                        print(i[4], ":", i[5])
                                        if j[8] % 10 == 0:
                                            a = j[8] / 2
                                        else:
                                            vo = j[8] / 2
                                            bo = vo % 5
                                            a = (j[8] / 2) - bo
                                        ti = 'Return Journey Fare Calculated Incorrectly'
                                        d = "Full Toll amount " + str(i[8]) + " was paid by " + i[2] + " at " + i[
                                            5] + " Dated: " + str(
                                            i[1]) + " with Trip no: " + i[
                                                7] + " .However for return journey within 24 hours dated : " + str(
                                            j[1]) + " with Trip no: " + j[7] + " , " + i[
                                                2] + " paid toll amount Rs " + str(
                                            j[8]) + " again .Hence Plz raise refund of disputed amount ."

                                        qre = "insert into toll_d (type,subtype,priority,severity,tripno,amount,title,description,code,plaza_name,lic_no,dtm) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                            t, s, p, se, tr2, a, ti, d, j[4], j[5], j[2], j[1])
                                        coo.execute(qre)
                                        re.pop(cnt)

                                    else:
                                        pass
                                elif cnt - count == 1 and j[1] - timedelta(minutes=20) > i[1] and i[
                                    1] + timedelta(days=1) >= j[1] and j[8] == i[8] and i[8] >= 40:

                                    rj += 1
                                    t = dbInfo.type
                                    s = 'Return Journey Fare Calculated Incorrectly'
                                    p = dbInfo.priority
                                    se = dbInfo.severity
                                    tr2 = j[7]
                                    # print(tr2)
                                    if j[8] % 10 == 0:
                                        a = j[8] / 2
                                    else:
                                        vo = j[8] / 2
                                        bo = vo % 5
                                        a = (j[8] / 2) - bo
                                    ti = 'Return Journey Fare Calculated Incorrectly'
                                    d = "Full Toll amount " + str(i[8]) + " was paid by " + i[2] + " at " + i[
                                        5] + " Dated: " + str(
                                        i[1]) + " with Trip no: " + i[
                                            7] + " .However for return journey within 24 hours  : " + str(
                                        j[1]) + " with Trip no: " + j[7] + " , " + i[2] + " paid toll amount Rs " + str(
                                        j[8]) + " again .Hence Plz raise refund of disputed amount ."

                                    qree = "insert into toll_d (type,subtype,priority,severity,tripno,amount,title,description,code,plaza_name,lic_no,dtm) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                        t, s, p, se, tr2, a, ti, d, j[4], j[5], j[2], j[1])
                                    coo.execute(qree)
                                    re.pop(cnt)

                    else:
                        pass
        db.commit()
        print('RJ Errors ', rj)



    except:
        traceback.print_exc()


def paired_toll_error():
    try:
        db = mysql.connector.connect(
            host=dbInfo.host,
            user=dbInfo.user,
            password=dbInfo.password,
            database=dbInfo.database
        )
        cursor = db.cursor(buffered=True)
        cur = db.cursor(buffered=True)
        cr = db.cursor(buffered=True)

        qry = "SELECT distinct lic_no from t_statement"
        cursor.execute(qry)
        resultc = cursor.fetchall()
        lst = []
        for q in resultc:
            lst.append(q[0])

        for s in lst:
            q = "select * from t_statement where lic_no='{}' and txn_dtm >='{}'order by txn_dtm;".format(s, dt)
            cur.execute(q)
            res = cur.fetchall()

            for i in paired_tolls:
                for cnt, j in enumerate(res):
                    if cnt + 1 < len(res) and i[0] == j[4] and i[1] == res[cnt + 1][4] and j[8] != 0 and res[cnt + 1][
                        8] != 0 and j[
                        1] + timedelta(hours=24) > res[cnt + 1][1]:
                        t = dbInfo.type
                        s = 'Toll fare calculation error'
                        p = dbInfo.priority
                        se = dbInfo.severity
                        tr2 = res[cnt + 1][7]
                        amount = res[cnt + 1][8]
                        ti = 'Paired toll discount not given'
                        d = "For connected plaza Toll paid in full by " + j[2] + " at Entry Toll Plaza Name: " + j[
                            5] + " with Trip No: " + j[7] + " Date: " + str(j[1]) + " there should be ZERO toll at " + \
                            res[cnt + 1][5] + " with Trip No: " + tr2 + " Dated : " + str(
                            res[cnt + 1][1]) + " --please raise refund of " + "Rs " + str(amount)
                        qr = "insert into toll_d (type,subtype,priority,severity,tripno,amount,title,description,code,plaza_name,lic_no,dtm) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                            t, s, p, se, tr2, amount, ti, d, res[cnt + 1][4], res[cnt + 1][5], res[cnt + 1][2],
                            res[cnt + 1][1])

                        cr.execute(qr)
                        res.pop(cnt + 1)

        db.commit()

        print("finished")
    except:
        traceback.print_exc()
def dd_error():
        try:
            db = mysql.connector.connect(
                host=dbInfo.host,
                user=dbInfo.user,
                password=dbInfo.password,
                database=dbInfo.database
            )
            cursor = db.cursor(buffered=True)
            cur = db.cursor(buffered=True)
            cr = db.cursor(buffered=True)

            qry = "SELECT distinct lic_no from t_statement"
            cursor.execute(qry)
            resultc = cursor.fetchall()
            lst = []
            for q in resultc:
                lst.append(q[0])

            dd = 0
            for ss in lst:
                q = "select * from t_statement where lic_no='{}' and txn_dtm >='{}'order by txn_dtm;".format(ss, dt)
                cur.execute(q)
                resv = cur.fetchall()
                # print(res)
                for count, i in enumerate(resv):
                    for cnt, j in enumerate(resv):
                        if j[1] >= i[1] and j[0] != i[0] and i[1] + timedelta(minutes=20) >= j[1] and i[4] == j[4] and \
                                j[8] > 0 and i[8] == j[8]:
                            dd += 1
                            t = dbInfo.type
                            s = 'Duplicate Transaction at Plaza/Double Debit'
                            p = dbInfo.priority
                            se = dbInfo.severity
                            tr2 = j[7]
                            a = j[8]
                            ti = 'Duplicate Transaction'
                            d = 'Shortly after the trip of ' + i[2] + " through toll booth " + i[
                                5] + ' with trip no :' + i[7] + " Dated : " + str(i[1]) + " when amount " + str(
                                i[8]) + " was paid " + ' another debit of amount ' + str(
                                j[8]) + ' was done from fastag account Dated: ' + str(j[1]) + " with Trip no: " + j[7]
                            qrr = "insert into toll_d (type,subtype,priority,severity,tripno,amount,title,description,code,plaza_name,lic_no,dtm) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                t, s, p, se, tr2, a, ti, d, j[4], j[5], j[2], j[1])

                            cr.execute(qrr)
                            resv.pop(cnt)

            db.commit()
            print("DD-E  ", dd)
        except :
            traceback.print_exc()


if __name__  ==  "__main__" :
    rj_error()
    dd_error()
    paired_toll_error()
