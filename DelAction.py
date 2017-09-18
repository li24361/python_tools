#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb

HOST   = '127.0.0.1'
USER   = 'root'
PASSWD = '123456'
DB     = 'promotion'
PORT   = 3306


# HOST   = '10.255.12.106'
# USER   = 'dev'
# PASSWD = '123'
# DB     = 'S61'
# PORT   = 3306

class DelAction(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def del_action_data(self):
        self.open_cursor()

        print '---------------------------- start  ----------------------------'

        rewards_sql = """
        select id from promotion.user_all_action_sync where action_type in (1,2,3)
        union
        select id from promotion.user_all_action_sync where action_type = 4 and created_time > '2015-09-10'
        """;

        count = self.cur.execute(rewards_sql)

        print "search id count:%d" % count

        del_sql = "delete from promotion.user_all_action_sync where id = %d" 

        sum = 0;
        for row in self.cur.fetchall(): 
            self.cur.execute(del_sql % row[0]) 
            # print del_sql % row[0]
            sum = sum +1 ;
            if (sum%500 == 0):
                print "commit %d------------------------------" % sum
                self.conn.commit()
            # print del_reward_sql % row[0]

        self.conn.commit()
        self.close_cursor()
        print '---------------------------- delete successfully----------------------------'


if __name__ == '__main__':
    DelAction = DelAction()
    DelAction.del_action_data()
