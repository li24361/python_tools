#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time
import csv


#hry
HOST   = '10.118.12.106'
USER   = 'rdon'
PASSWD = 'readonlypwd'
DB     = ''
PORT   = 3306

# HOST   = '127.0.0.1'
# USER   = 'root'
# PASSWD = '123456'
# DB     = 'promotion'
# PORT   = 3306

class UserAccount(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def get_ids(self,f):
        s = set()
        csvfile = file(f, 'rb')
        reader = csv.reader(csvfile)

        for line in reader:
            s.add(line[0])

        csvfile.close() 
        return s

    def find(self):
        start =time.clock()

        wlzh = set()
        tzyh = set()
        hryyh = set()


        wlzh = self.get_ids('wlzh.csv')
        tzyh = self.get_ids('tz.csv')
        hryyh = self.get_ids('hryid.csv')

        hry_wl = wlzh & hryyh
        hry_tz = tzyh & hryyh

        print "<-----------------result----------------->"
        writer = csv.writer(file('wl11.csv', 'wb'))
        for id in hry_wl:
            row=[id]
            writer.writerow(row)

        writer = csv.writer(file('tz11.csv', 'wb'))
        for id in hry_tz:
            row=[id]
            writer.writerow(row)

        # f = open('./xysql.txt','w')
        # # print len(result)

        # insert_sql = "INSERT INTO s61.tzrtbxy (USERID, BIDID, XYID, XYVERSION, HTBH) VALUES (%s, %s, %s, 0, \'S-%s-%s-%d\');\n"

        # d={}

        # for row in self.cur.fetchall():
        #     # print row[0],row[1],row[2],row[3]
        #     d[str(row[3])] = d.get(str(row[3]),990000)+1
        #     htbh = ''
        #     xyid = 0
        #     if row[0]==19:
        #         xyid = 3022
        #         htbh = 'XFDYDF'
        #     if row[0] ==20:
        #         xyid = 3023
        #         htbh = 'GGSRZ'
        #     # print insert_sql % (row[1],row[2],xyid,htbh,row[3],d[row[3]])
        #     f.write(insert_sql % (row[1],row[2],xyid,htbh,row[3],d[row[3]]))
        #     # f.write(sql % (user_ids[0,]))

        # f.close()
        end = time.clock()
   
        print '----------Running time: %s Seconds-----------------' % (end - start)




if __name__ == '__main__':
    UserAccount = UserAccount()
    UserAccount.find()
