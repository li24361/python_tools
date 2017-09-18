#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLImportExport

#product
HOST = '10.251.12.106'
USER = 'dev'
PASSWD = '123'
DB = ''
PORT = 3306


#hry
HOST1 = '10.135.111.22'
USER1 = 'dev'
PASSWD1 = '123'
DB1 = ''
PORT1 = 3306


class PullData(object):


    def pull_all(self):
        print "------------------start------------------ "
        con = MySQLImportExport.MySQLImporter(HOST, USER, PASSWD, DB).get_connection()
        con1 = MySQLImportExport.MySQLExporter(HOST1, USER1, PASSWD1, DB1).get_connection()
        bid_id = '23060'
        print "bid_id="+bid_id
        # self.pull_data(con, con1, 's65.ocp_message', {'bid_id': bid_id})
        
        self.pull_data(con, con1, 's62.t6230', {'F01': bid_id})
        self.pull_data(con, con1, 's62.t6231', {'F01': bid_id})
        self.pull_data(con, con1, 's62.t6250', {'F02': bid_id})
        self.pull_data(con, con1, 's62.t6238', {'F01': bid_id})
        self.pull_data(con, con1, 's65.t6504', {'F03': bid_id})
        self.pull_data(con, con1, 's62.t6251', {'F03': bid_id})
        self.pull_data(con, con1, 's61.t6110', " F01 IN (SELECT F02 FROM s62.t6230 WHERE F01="+bid_id+")")
        self.pull_data(con, con1, 's61.t6110', " F01 IN (SELECT F03 FROM s62.t6250 WHERE F02="+bid_id+")")
        self.pull_data(con, con1, 's61.T6101', " F02 IN (SELECT F03 FROM s62.t6250 WHERE F02="+bid_id+")")
        con1.query("UPDATE s62.t6250 SET F08='F' WHERE F02="+bid_id)
        con1.query("UPDATE s62.t6230 SET F20='DFK' WHERE F01="+bid_id)
        print "------------------end------------------ "

    def pull_data(self, con, con1, table_name, condition):
        print table_name+" pull start!"
        db = MySQLImportExport.MySQLExporter()
        db.set_connection(con)
        db.set_table(table_name)
        list = db.get_fields([], condition)

        db1 = MySQLImportExport.MySQLImporter()
        db1.set_connection(con1)
        db1.set_table(table_name)
        # 添加过滤字段
        db1.set_filters({'F01', 'F02', 'F03'})
        # db1.set_filters({'bid_id','id'})
        for l in list:
            # db1.create_insert_query(l)
            db1.import_item(l, False)
        print table_name+" pull finish! total:"+str(len(list))

if __name__ == '__main__':
    PullData = PullData()
    PullData.pull_all()
