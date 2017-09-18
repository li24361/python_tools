#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time
import sys

#promotion
HOST   = '10.255.12.107'
USER   = 'dev'
PASSWD = '123'
DB     = 'S62'
PORT   = 3307



class checkbid(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)
        self.bid1 = sys.argv[1]
        self.bid2 = sys.argv[2]

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def check_data(self):
        print '------search bid: (%s vs %s)---------' % (self.bid1, self.bid2)

        self.check_bid()


    def check_bid(self):
        self.open_cursor()

        rewards_sql = '''
        select a.*,b.*,c.* from S62.T6230 a 
        left join S62.T6231 b on a.F01 = b.F01 
        left join S62.T6238 c on a.F01 = c.F01 where a.F01 in (%s,%s)''';

        count = self.cur.execute(rewards_sql , (self.bid1,self.bid2))
        desc = self.cur.description
        # print 'cur.description:',desc


        rows= self.cur.fetchall()
        # print rows[0]
        # print rows[1] 

        for i,f in enumerate(rows[0]):
            if(str(rows[0][i])==str(rows[1][i])):
                continue
            print ('%s,%20s,%20s') % (str(desc[i][0]),str(rows[0][i]),str(rows[1][i]))



        self.close_cursor()

  



if __name__ == '__main__':
    if len(sys.argv)<3:
        print 'please input bid1 and bid2! example: python checkbid.py 231 234'
        exit(1)
    checkbid = checkbid()
    checkbid.check_data()
