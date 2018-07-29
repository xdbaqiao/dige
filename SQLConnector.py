#!/usr/bin/env python2
# coding: utf-8
__doc__ = 'mysql/mssql database connect function'

import re
import csv
import MySQLdb
from datetime import datetime

class MySQLConnect: 
    def __init__(self, host, username, passwd, database):
        try:
            self.connect = MySQLdb.connect(
                    host=host, 
                    user=username,
                    passwd=passwd,
                    charset='utf8')
            self.cursor = self.connect.cursor()
            self.connect.select_db(database)
        except Exception, e:
            common.logger.error(e)
            self.cursor = None

    def query(self, sql):
        if not self.cursor:
            return
        print('Execute SQL: %s' % sql)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        self.connect.commit()
        return results

    def insert(self, table, bag):
        if self.cursor:
            sql = 'INSERT INTO %s(%s) VALUES (%s);' % (
                    table, 
                    ','.join(bag.keys()), 
                    ','.join(['%s']*len(bag))
                    )
            self.cursor.execute(sql, tuple(bag.values()))
            self.connect.commit()

    def update(self, table, value, cond):
        if self.cursor:
            sql = 'UPDATE %s set %s where %s;' % (
                    table, 
                    value,
                    cond
                    )
            print('Execute SQL: %s' % sql)
            self.cursor.execute(sql)
            self.connect.commit()

    def execute(self, sql):
        if not self.cursor:
            return
        print('Execute SQL: %s' % sql)
        self.cursor.execute(sql)
        self.connect.commit()

    def close(self):
        self.connect.close()

def connect_database(ip, database='openaccount'):
    connect = None
    if ip == 'localhost':
        connect = MySQLConnect('localhost', 'root', 'root@123', database)
    elif ip == '172.16.5.51':
        connect = MsSQLConnect('172.16.5.51', 'sa', 'jty5585354?', database)
    return connect

def load_to_database(tbname, file, truncate=True, db='etherscan', delete=True):
    conn = MySQLConnect('localhost', 'root', 'yexinjing', 'etherscan')
    if delete:
        sql = 'delete from %s.%s where sdate = %s' % (db, tbname, datetime.now().strftime('%Y%m%d'))
        conn.execute(sql)
    if truncate:
        sql = 'truncate table %s.%s' % (db, tbname)
        conn.execute(sql)

    sql = '''load data local infile '%s' replace into table %s.%s character set utf8 fields terminated by '\\t' ''' % (file, db, tbname)
    conn.execute(sql)
    conn.close()

if __name__ == '__main__':
    mysqlconnect = MySQLConnect('localhost', 'root', 'root@123', 'openaccount')
    print mysqlconnect.query('select * from indicator_pdp_config')
    mysqlconnect.close()
