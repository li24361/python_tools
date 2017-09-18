#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time



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

class FixTzxy(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def find(self):
        start =time.clock()

        sql = "SELECT t2.F04,t1.F03,t1.F01,DATE_FORMAT(t1.F06,'%Y%m%d') FROM S62.t6250 t1 LEFT JOIN S62.t6230 t2 ON t1.F02=t2.F01 WHERE t1.F01 NOT IN (SELECT bidid FROM S61.`tzrtbxy`) AND t1.F08='S' AND t1.F02<>692"

        self.open_cursor()

        self.cur.execute(sql)

        print "<-----------------result----------------->"


        f = open('./xysql.txt','w')
        # print len(result)

        insert_sql = "INSERT INTO s61.tzrtbxy (USERID, BIDID, XYID, XYVERSION, HTBH) VALUES (%s, %s, %s, 1, \'S-%s-%s-%d\');\n"

        d={}

        for row in self.cur.fetchall():
            # print row[0],row[1],row[2],row[3]
            d[str(row[3])] = d.get(str(row[3]),990000)+1
            htbh = ''
            xyid = 0
            if row[0]==19:
                xyid = 3022
                htbh = 'XFDYDF'
            if row[0] ==20:
                xyid = 3023
                htbh = 'GGSRZ'
            # print insert_sql % (row[1],row[2],xyid,htbh,row[3],d[row[3]])
            f.write(insert_sql % (row[1],row[2],xyid,htbh,row[3],d[row[3]]))
            # f.write(sql % (user_ids[0,]))

        f.close()
        end = time.clock()
   
        print '----------Running time: %s Seconds-----------------' % (end - start)




if __name__ == '__main__':
    FixTzxy = FixTzxy()
    FixTzxy.find()
