# -*- coding:utf-8 -*-
__author__ = 'LiuYang'

import MySQLdb

class SuperDAO(object):

    def __init__(self, host, db, user, password):
        self.host = host
        self.db = db
        self.user = user
        self.password = password

    '''
    保存一条记录
    '''
    def save_record(self, table_name, record):
        db = MySQLdb.connect(self.host, self.user, self.password, self.db, charset='utf8')
        cursor = db.cursor()
        try:
            placeholders = ', '.join(['%s'] * len(record))
            columns = ', '.join(record.keys())
            sql = "insert into %s( %s ) values ( %s )" % (table_name, columns, placeholders)
            cursor.execute(sql, record.values())
        except Exception, e:
            print e
        db.commit()
        cursor.close()
        db.close()

    '''
    保存多条记录
    '''
    def save_records(self, table_name, records):
        db = MySQLdb.connect(self.host, self.user, self.password, self.db, charset='utf8')
        cursor = db.cursor()
        try:
            for record in records:
                placeholders = ', '.join(['%s'] * len(record))
                columns = ', '.join(record.keys())
                sql = "insert into %s( %s ) values ( %s )" % (table_name, columns, placeholders)
                cursor.execute(sql, record.values())
        except Exception, e:
            print e
        db.commit()
        cursor.close()
        db.close()