#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time

#promotion
HOST   = '10.118.12.2'
USER   = 'user_dev'
PASSWD = '4rfv%TGB^'
DB     = 'promotion'
PORT   = 13306


#hry
HOST1   = '10.118.12.106'
USER1   = 'rdon'
PASSWD1 = 'readonlypwd'
DB1     = ''
PORT1   = 3306

# HOST   = '127.0.0.1'
# USER   = 'root'
# PASSWD = '123456'
# DB     = 'promotion'
# PORT   = 3306

class FindLostRegInfo(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)
        self.start_time = '2015-9-21 00:00:00'
        self.end_time = '2015-10-09 09:30:00'
        self.hry_user_ids = []
        self.promotion_user_ids=[]

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def find(self):
        start =time.clock()

        print '------search time: (%s -----> %s)---------' % (self.start_time, self.end_time)
        # self.check_actions()

        self.check_promotion_all()

        print "----------------------------------"

        self.check_hry_all()

        print "<-----------------result----------------->"

        result =  set(self.hry_user_ids).difference(set(self.promotion_user_ids))

        f = open('./user_id.txt','w')

        # print len(result)


        insert_sql = "INSERT INTO promotion.user_all_action_sync (user_source,user_id , registration_time, action_time,created_time,action_type, payment_type, status) VALUES ('2',\'%s\',\'%s\',\'%s\',now(),'1', '3', '1');\n"

        for user_id in result:
            # print user_id[0],user_id[1]
            f.write(insert_sql % (user_id[0],str(user_id[1]),str(user_id[1])))
            # f.write(sql % (user_ids[0,]))

        f.close()
        end = time.clock()
   
        print '----------Running time: %s Seconds-----------------' % (end - start)



    def check_promotion_all(self):
        self.open_cursor()


        all_actions_sql = "SELECT user_id,action_time FROM promotion.user_all_action_sync where status =1 and id<>transaction_id and action_time between %s and %s"
        self.cur.execute(all_actions_sql , (self.start_time,self.end_time))
        # print '---------action_type,rule_id,count:--------'
        self.promotion_user_ids =  self.cur.fetchall()

        # print self.promotion_user_ids
        self.close_cursor()        

    def check_hry_all(self):
        self.conn1 = MySQLdb.connect(host=HOST1, user=USER1, passwd=PASSWD1, db=DB1, port=PORT1)
        self.cur1 =self.conn1.cursor()

        #注册 
        sql1 = "SELECT concat(F01,''),F09 FROM S61.T6110 where F09  between  %s AND  %s"
        self.cur1.execute(sql1,(self.start_time,self.end_time))
        self.hry_user_ids = self.cur1.fetchall()

        # print self.hry_user_ids


        self.cur1.close()

if __name__ == '__main__':
    FindLostRegInfo = FindLostRegInfo()
    FindLostRegInfo.find()
