#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time
import sys

#准绳韩
HOST = '10.255.12.106'
USER = 'rdon'
PASSWD = 'readonlypwd'
DB = ''
PORT = 3306


#qa1
HOST1 = '10.251.12.106'
USER1 = 'dev'
PASSWD1 = '123'
DB1 = ''
PORT1 = 3306



class checkbid(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)
        self.conn1 = MySQLdb.connect(host=HOST1, user=USER1, passwd=PASSWD1, db=DB1, port=PORT1)
        self.bid1 = sys.argv[1]
        self.bid2 = sys.argv[2]

    def close_cursor(self):
        self.cur.close()
        self.cur1.close()

    def check_data(self):
        print '------search bid: (%s vs %s)---------' % (self.bid1, self.bid2)
        self.check_bid()


    def check_bid(self):
        self.cur = self.conn.cursor()
        self.cur1 = self.conn1.cursor()

        # self.pull_data(con, con1, 's62.t6230', {'F01': bid_id})
        # self.pull_data(con, con1, 's62.t6231', {'F01': bid_id})
        # self.pull_data(con, con1, 's62.t6250', {'F02': bid_id})
        # self.pull_data(con, con1, 's62.t6238', {'F01': bid_id})
        # self.pull_data(con, con1, 's65.t6504', {'F03': bid_id})
        # self.pull_data(con, con1, 's62.t6251', {'F03': bid_id})

        sqls=[
            'select * from s62.t6230 where F01=%s',
            'select * from s62.t6231 where F01=%s',
            'select * from s62.t6252 where F02=%s',
            'select * from s62.t6251 where F03=%s',
            'select * from s62.t6238 where F01=%s',
            'select * from s65.t6505 where F03=%s',
            'select * from s65.ocp_message where bid_id=%s'
        ]

        for sql in sqls:
            print '------------------------------------------------------------------------------'
            print sql
            self.cur.execute(sql , (self.bid1,))
            desc = [tuple[0] for tuple in self.cur.description] 
            rows= self.cur.fetchall()
            if(len(rows)==0):
                print ('stg1 not found %s') % self.bid1
                break
            self.cur1.execute(sql , (self.bid2,))
            desc1 = [tuple[0] for tuple in self.cur1.description] 
            rows1= self.cur1.fetchall()
            if(len(rows1)==0):
                print ('qa1 not found %s') % self.bid2
                break
            r = rows[0]
            if(len(rows[0])<len(rows1[0])):
                r = rows[0]
                print ('database column is not same , stg miss:%s') %  set(desc1).difference(set(desc))
            if(len(rows[0])>len(rows1[0])):
                r = rows1[0]
                print ('database column is not same , qa miss:%s') %  set(desc).difference(set(desc1))
            for i,f in enumerate(r):
                if(str(rows[0][i])==str(rows1[0][i])):
                    continue
                print ('different:%s,%20s,%20s') % (str(desc[i]),str(rows[0][i]),str(rows1[0][i]))
        self.close_cursor()

  



if __name__ == '__main__':
    if len(sys.argv)<3:
        print 'please input bid1 and bid2! example: python checkbid.py 231 234'
        exit(1)
    checkbid = checkbid()
    checkbid.check_data()
