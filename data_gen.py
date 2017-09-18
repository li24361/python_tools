#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb

HOST   = '10.135.111.22'
USER   = 'dev'
PASSWD = 'dev'
DB     = 'S71'
PORT   = 3307


# HOST   = '10.255.12.106'
# USER   = 'dev'
# PASSWD = '123'
# DB     = 'S61'
# PORT   = 3306

class MoveData(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def move_promotion_data(self):
        self.open_cursor()

        sql = "INSERT INTO `s71`.`action_record` (`USERID`, `COOKIES`, `DATE`, `REMOTE_ADDR`, `REMOTE_HOST`, `LOCAL_ADDR`, `LOCAL_NAME`, `LOCAL_PORT`, `REQUEST_URL`, `METHOD`, `PROTOCOL`, `CONTEXT_PATH`) VALUES ('3930', 'Name=ACCOUNT_NAME&Value=QYZH00000003929&path=null&domain=null&comment=null;Name=ACCOUNT_TYPE&Value=2&path=null&domain=null&comment=null;Name=p2p_guid&Value=ed5c8d10-9b4d-43b4-a416-ff9b076b7a3a&path=null&domain=null&comment=null;Name=136a3d03-9748-4f83-a54f-9b2a93f979a0&Value=46294e84-2c59-411e-a1ea-44421a042ea3&path=null&domain=null&comment=null;', '2015-12-30 17:14:21', '127.0.0.1', '127.0.0.1', '127.0.0.1', 'www.hry.com', '8080', 'http://localhost:8080/user/credit/repaying.htm', 'GET', 'HTTP/1.1', '/user');"

        i=0
        while i<10000000 :
            self.cur.execute(sql)
            i=i+1
            if (i%1000==0):
                self.conn.commit()
                print 'insert ',i
            # print '> ', sql1





        self.close_cursor()
        print 'end'


if __name__ == '__main__':
    MoveData = MoveData()
    MoveData.move_promotion_data()
