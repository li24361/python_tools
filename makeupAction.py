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
        select user_all_action_sync.id from user_all_action_sync 
		where id not  in (select id from user_all_action_sync where promotion_activity_rule_id is not null ) 
		and id  in (select user_action_sync_id from rewards_actions_mapper)
        """;

        count = self.cur.execute(rewards_sql)

        print "search id count:%d" % count

        update_sql = "UPDATE  promotion.user_all_action_sync SET `promotion_activity_rule_id`='2' where id = %d" 

        sum = 0;
        for row in self.cur.fetchall(): 
            self.cur.execute(update_sql % row[0]) 
            # print update_sql % row[0]
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
