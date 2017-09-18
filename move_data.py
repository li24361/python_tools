#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb

HOST   = '10.135.111.22'
USER   = 'dev'
PASSWD = 'dev'
DB     = 'S61'
PORT   = 3306


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

        sql = "select F01, F02 from T6111 order by F01";
        
        promotion_code_sql = "insert ignore into promotion.user_promotion (user_source, user_id, promotion_id, created_time, updated_time, status) values (2, '%s', '%s', now(), now(), 1)"
        
        relation_sql = "select t1.F01,t2.F01,t3.F09 from S61.T6111 t1 inner join S61.T6111 t2 on t1.F02=t2.F03 left join S61.T6110 t3 on t2.F01=t3.F01 order by t1.F01;"

        promotion_sql = "insert ignore into promotion.promotion_relation (promoter_source, promoter_id, promoted_source, promoted_id, platform_id, promotion_source, promotion_activity_id, action_type, action_time, created_time, status) values (2, '%s', 2, '%s', 1, 7, 999, 1, '%s', now(), 1) "
        
        count = self.cur.execute(sql)
        print 'promotion code sql:> ', count

        sqlfile = open('./promotion.sql','w')


        for row in self.cur.fetchall():
            sql1 = promotion_code_sql % (row[0], row[1])
            # print '> ', sql1
            sqlfile.write(sql1+";\n");
            # self.cur.execute(lcj_sql)


        # count = self.cur.execute(relation_sql)
        # print 'promotion relationship sql> ', count

        # for row in self.cur.fetchall():
        #     sql2 = promotion_sql % (row[0], row[1], row[2])
        #     # print '> ', sql2
        #     sqlfile.write(sql2+";\n");
        #     # self.cur.execute(sd_sql)

        # self.conn.commit()
        sqlfile.close()
        self.close_cursor()
        print 'pleace execute generated sql in promotion.sql'


if __name__ == '__main__':
    MoveData = MoveData()
    MoveData.move_promotion_data()
