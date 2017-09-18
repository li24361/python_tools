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

class CheckRewards(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT)
        self.start_time = '2015-09-20 00:00:00'
        self.end_time = '2015-12-01 19:10:00'

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def check_data(self):
        start =time.clock()

        print '------search time: (%s -----> %s)---------' % (self.start_time, self.end_time)
        # self.check_actions()
        print '<====================== Promotion Action====================>'
        self.check_promotion_all()
        print '<========================= HRY Action=======================>'
        # self.check_hry_all()

        end = time.clock()
   
        print '----------Running time: %s Seconds-----------------' % (end - start)

    def check_actions(self):
        self.open_cursor()

        d = {}

        rewards_sql = "SELECT id,promotion_activity_rule_id,action_type FROM promotion.user_all_action_sync  where status =1 and created_time between %s and %s";

        count = self.cur.execute(rewards_sql , (self.start_time,self.end_time))

        print 'action count:%d' % count

        action_map_sql = "SELECT rewards_id,id FROM promotion.rewards_actions_mapper where status=1 and  user_action_sync_id = %s" 

        for i,row in enumerate(self.cur.fetchall()): 

            if (i%500 == 0):
                print "checking actions--------> %d" % i

            self.cur.execute(action_map_sql , [row[0]])

            for row1 in self.cur.fetchall():

                action_sql = "SELECT id,rewards,promotion_activity_rule_id FROM promotion.rewards where status=1 and id = %s "
                self.cur.execute(action_sql , [row1[0]]) 

                for row2 in self.cur.fetchall(): 
                    # index = str(row2[0]).find(",");

                    # new_rule_id = ""
                    # if(index!=-1):
                    #     new_rule_id = row2[0][0:index]
                    # else:
                    #     new_rule_id = "null"
                    d['reward-rule-'+str(row2[2])] = d.get('action-rule-'+str(row2[2]),0)+1;      
                    d['reward-'+str(row2[1])] = d.get('reward-'+str(row2[1]),0)+1

                    # self.conn.commit()
                # d['action_mapper_count'] = d.get('action_mapper_count',0)+1

            d['action-rule-'+str(row[1])] = d.get('action-rule-'+str(row[1]),0)+1;
            d['action-type-'+str(row[2])] = d.get('action-type-'+str(row[2]),0)+1;
            d['actions'] = d.get('actions',0)+1           

            # d['relation_count'] = d.get('relation_count',0)+1           

            # print del_reward_sql % row[0]
        print '---------rewards total show:---------------'
        for k,v in d.items():
            print '%-25s : %-15s'%(k,v) 
        self.close_cursor()

    def check_promotion_all(self):
        self.open_cursor()

        #统计奖励笔数最多
        user_count_sql = "select user_id,count(*),sum(rewards) from rewards where STATUS =1 and serial_id is null  AND created_time BETWEEN %s AND %s GROUP BY user_id ORDER BY COUNT(*) DESC limit 10"
        self.cur.execute(user_count_sql , (self.start_time,self.end_time))
        print '---------rewards count top 10:---------------'
        for count_row in self.cur.fetchall():
            print '%-15s %-5s %-10s' % (count_row[0],count_row[1],count_row[2])

        user_sum_sql = "select user_id,count(*),sum(rewards) from rewards where STATUS =1 and serial_id is null  AND created_time BETWEEN %s AND %s GROUP BY user_id ORDER BY sum(rewards) DESC limit 10"
        self.cur.execute(user_sum_sql , (self.start_time,self.end_time))
        print '---------rewards sum top 10:---------------'
        for sum_row in self.cur.fetchall():
            print '%-15s %-5s %-10s' % (sum_row[0],sum_row[1],sum_row[2])

        all_relaition_sql = "SELECT promotion_activity_id, COUNT(promotion_relation_id) FROM rewards_promotion_mapper WHERE created_time BETWEEN %s AND %s GROUP BY promotion_activity_id"
        self.cur.execute(all_relaition_sql , (self.start_time,self.end_time))
        print '---------relation_action_id,count:---------'
        for relation_row in self.cur.fetchall():
            print '%-15s %-5s' % (relation_row[0],relation_row[1])


        all_actions_sql = "SELECT action_type,promotion_activity_rule_id,count(1) FROM promotion.user_all_action_sync where status =1 and id<>transaction_id and action_time between %s and %s GROUP BY action_type,promotion_activity_rule_id";
        self.cur.execute(all_actions_sql , (self.start_time,self.end_time))
        print '---------action_type,rule_id,count:--------'
        for action_row in self.cur.fetchall():
            print '%-15s %-5s %-10s' % (action_row[0],action_row[1],action_row[2])

        self.close_cursor()        

    def check_hry_all(self):
        self.conn1 = MySQLdb.connect(host=HOST1, user=USER1, passwd=PASSWD1, db=DB1, port=PORT1)
        self.cur1 =self.conn1.cursor()

        #注册 
        sql1 = "select count(*) from S61.T6110 where F09  between  %s AND  %s"
        self.cur1.execute(sql1,(self.start_time,self.end_time))
        for row in self.cur1.fetchall():
            print 'action-1-count : %d' % row[0]
        
        #实名认证 
        sql2 = "select count(*) from  S61.T6141 t1, S61.T6110 t2  where t1.F01 = t2.F01  and t1.F10    between  %s AND  %s"
        self.cur1.execute(sql2,(self.start_time,self.end_time))
        for row in self.cur1.fetchall():
            print 'action-2-count : %d' % row[0]
        
        #绑定银行卡
        sql3 = "select count(*) from  S61.T6114 t1, S61.T6110 t2 where t1.F02 = t2.F01 and t1.F09  between  %s AND %s"
        self.cur1.execute(sql3,(self.start_time,self.end_time))
        for row in self.cur1.fetchall():
            print 'action-3-count : %d' % row[0]
        
        #投资
        sql4 = "select count(*) from S62.T6250 t1, S62.T6230 t3,S61.T6110 t4  where t3.F01=t1.F02 and t1.F03=t4.F01 and t1.f06 between  %s AND  %s"
        self.cur1.execute(sql4,(self.start_time,self.end_time))
        for row in self.cur1.fetchall():
            print 'action-4-count : %d' % row[0]

        self.cur1.close()

if __name__ == '__main__':
    CheckRewards = CheckRewards()
    CheckRewards.check_data()
