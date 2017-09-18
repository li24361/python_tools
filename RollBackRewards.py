#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import MySQLdb
import time

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

class RollBackRewards(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB, port=PORT, charset='utf8')

    def open_cursor(self):
        self.cur = self.conn.cursor()

    def close_cursor(self):
        self.cur.close()

    def move_promotion_data(self):
        start =time.clock()

        self.open_cursor()

        print '----------------------------start roll back reward----------------------------'

        rewards_sql = "SELECT id FROM promotion.rewards where created_time >'2015-9-10 00:00:00'";

        # print rewards_sql

        count = self.cur.execute(rewards_sql)

        print "search rewards count : %d" % count

        rewards_count = 0
        action_count = 0
        action_mapper_count = 0
        relation_mapper_count = 0

        action_map_sql = "SELECT user_action_sync_id,id FROM promotion.rewards_actions_mapper where rewards_id = %d" 

        for row in self.cur.fetchall(): 
            self.cur.execute(action_map_sql % row[0]) 
            # print action_map_sql % row[0]

            for row1 in self.cur.fetchall(): 

                check_sql1 = "SELECT count(*) FROM promotion.rewards_actions_mapper where created_time>'2015-9-10 00:00:00' and user_action_sync_id = %d "
                self.cur.execute(check_sql1 % row1[0])
                count1 = self.cur.fetchone()[0]

                # print count1,' ',check_sql1 % row1[0]
                check_sql2 = "SELECT count(*) FROM promotion.rewards_promotion_mapper where rewards_id = %d "
                self.cur.execute(check_sql2 % row[0])
                count2 = self.cur.fetchone()[0]
                # print count2,' ',check_sql2 % row[0]


                action_sql = "SELECT promotion_activity_rule_id FROM promotion.user_all_action_sync where id = %d "
                self.cur.execute(action_sql % row1[0]) 

                # fix bug：1个action 对应2个reward 所以，要判断一下，如果只剩1个reward，并且是基于推广关系的，表明action已经被处理
                if ((count1 >1 and count2 > 0) or count2 ==0): 
                    for row2 in self.cur.fetchall(): 
                        index = str(row2[0]).find(",");

                        new_rule_id = ""
                        if(index!=-1):
                            new_rule_id = row2[0][0:index]
                        else:
                            new_rule_id = "null"

                        update_rule_sql = "update promotion.user_all_action_sync set promotion_activity_rule_id=%s where id = %d"

                        # print update_rule_sql % (new_rule_id, row1[0])

                        self.cur.execute(update_rule_sql % (new_rule_id, row1[0]))
                        action_count = action_count+1;
                        # self.conn.commit()


                del_action_sql = "delete from  promotion.rewards_actions_mapper  where  id = %d"
                action_mapper_count = action_mapper_count+1
                self.cur.execute(del_action_sql % row1[1]) 

            del_relation_map_sql = "delete from promotion.rewards_promotion_mapper where rewards_id = %d"
            self.cur.execute(del_relation_map_sql % row[0])
            relation_mapper_count = relation_mapper_count+1 

            del_reward_sql = "delete from promotion.rewards  where id= %d";
            self.cur.execute(del_reward_sql % row[0]) 
            rewards_count = rewards_count+1
            if (rewards_count%500==0):
                self.conn.commit()
                print 'processing : %.2f %%' % (rewards_count*100/count)
            # print del_reward_sql % row[0]

        self.conn.commit()  
        self.close_cursor()
        end = time.clock()
        print('Running time: %s Seconds'%(end-start))
        print "del rewards:%d ,del action_mapper:%d ,del relation_mapper: %d ,update action rule Id: %d" %(rewards_count,action_mapper_count,relation_mapper_count,action_count)


if __name__ == '__main__':
    RollBackRewards = RollBackRewards()
    RollBackRewards.move_promotion_data()
