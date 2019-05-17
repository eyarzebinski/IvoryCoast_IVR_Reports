# encoding=utf8
import csv
import os
import pickle
import time
from datetime import datetime
from datetime import timedelta

import pymysql

start = time.time()
cnx = pymysql.connect(user='trecc_user', password='treccuser',
                      host='10.20.20.3',
                      database='treccprod', charset='utf8')
cursor = cnx.cursor()
FILE_NAME = 'cdr_id_key.txt'


def get_start_cdr_id():
    dict2 = {}
    if os.path.exists(FILE_NAME):
        file = open(FILE_NAME, 'r+')
        dict2 = pickle.load(file)
    print dict2
    return dict2.get('last_id', 0)


def write_last_id(cdr_id):
    dict = {'last_id': cdr_id}  # this might cause an error, shoudl this be cdr_id or clid?
    file = open(FILE_NAME, 'w')
    pickle.dump(dict, file)
    file.close()

    # def write_last_id(interaction_id):
    # dict = {'last_id': interaction_id}
    # file = open(FILE_NAME, 'w')
    # pickle.dump(dict, file)
    # file.close()


#new. evelyn fixed?
def find_start_id(table, date, id_name="id", created_at="created_at"):
    sql = "SELECT %s FROM %s ORDER BY %s DESC LIMIT 1" % (id_name, table, id_name)
    #sql = "SELECT %s FROM %s WHERE %s>='%s' LIMIT 1" % (id_name, table, created_at, date)
    #print "CHECK THIS: ", sql
    cursor.execute(sql)
    result = cursor.fetchone()
    return result[0]

##old
#def find_start_id(table, date, id_name="clid", created_at="calldate"):  # this might cause an error
#    sql = "SELECT %s FROM %s WHERE %s>='%s' LIMIT 1" % (id_name, table, created_at, date)
#    # print sql
#    cursor.execute(sql)
#    result = cursor.fetchone()
#    return result[0]


def find_end_id(table, date, id_name="clid", created_at="calldate"):  # this might cause an error
    sql = "SELECT %s FROM %s ORDER BY %s DESC LIMIT 1" % (id_name, table, id_name)
    # print sql
    cursor.execute(sql)
    result = cursor.fetchone()
    return result[0]


def output_csv(sql, file_name, read_type="w+"):
    cnx2 = pymysql.connect(user='trecc_user', password='treccuser',
                           host='10.20.20.3',
                           database='treccprod', charset='utf8')
    cursor2 = cnx2.cursor()
    cursor2.execute(sql)
    # print sql
    result = cursor2.fetchall()
    c = csv.writer(open("%s.csv" % file_name, read_type), delimiter=';')
    if read_type == 'w+':
        keys = [i[0] for i in cursor.description]
        c.writerow(keys)
    for x in result:
        x = ['NULL' if x1 is None else x1 for x1 in x]
        c.writerow([unicode(s).encode("utf-8") for s in x])

    cnx2.close()
    print file_name


STUDY_START_TIME = "2019-02-09 13:00:00"
# CDR_START_DATE = "2019-02-20 14:01:28"

# ANSWER_START_ID = find_start_id('user_answer_stats', STUDY_START_TIME)
# CDR_ID = find_start_id('cdr_ivr01', CDR_START_DATE, "clid", "calldate")
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
today_midnight = datetime.now()
TODAY = datetime.strftime(today_midnight, TIMESTAMP_FORMAT)
YESTERDAY = datetime.strftime(today_midnight - timedelta(1), TIMESTAMP_FORMAT)
CDR_START = YESTERDAY
CDR_END = TODAY

CDR_START_ID = find_start_id('cdr_ivr01', CDR_START, "clid", "calldate")
CDR_END_ID = find_end_id('cdr_ivr01', CDR_END, "clid", "calldate")
# USER_START_ID = find_start_id('users', STUDY_START_TIME)

CDR_SQL = """SELECT * FROM treccprod.cdr_ivr01 
WHERE treccprod.cdr_ivr01.clid >= %s AND treccprod.cdr_ivr01.clid < %s
ORDER BY treccprod.cdr_ivr01.clid ASC ;"""

# WHERE treccprod.cdr_ivr01.clid >= %s AND treccprod.cdr_ivr01.clid < %s


print TODAY, YESTERDAY
print " "
cnx.close()

# print CDR_SQL
# output_csv(CDR_SQL, "cdr-output")

# print CDR_SQL
CDR_START = get_start_cdr_id()
if CDR_START:
    CDR_START_ID = CDR_START
LIMIT = 30000
END = 0
print "Full range: ", CDR_START_ID, CDR_END_ID

print CDR_END_ID

for i in range(CDR_START_ID, CDR_END_ID, LIMIT):
    START = i
    END = i + LIMIT
    #CDR_SQL_NEW = CDR_SQL % (START)
    CDR_SQL_NEW = CDR_SQL % (START, END)
    write_last_id(END)
    print "Current range: ", START, END
    output = output_csv(CDR_SQL_NEW,
                        "cdr-file-%s-%s-%s" % (CDR_START_ID, CDR_END_ID, LIMIT),
                        read_type="a+")
if END:
    if END > CDR_END_ID:
        #END -= LIMIT
        END = CDR_END_ID
    CDR_SQL_NEW = CDR_SQL % (END, CDR_END_ID)
    print "Final range: ", END, CDR_END_ID #changed START to END
    write_last_id(CDR_END_ID)
    #output = output_csv(CDR_SQL_NEW,
    #                    "cdr-file-%s-%s-%s" % (CDR_START_ID, CDR_END_ID, LIMIT),
    #                    read_type="a+")


print ""
