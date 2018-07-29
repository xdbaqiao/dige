#!/usr/bin/env python2
# coding: utf-8

import re
from SQLConnector import MySQLConnect

def stat_data():
    # 统计当日、5日、20日等涨跌幅数据
    conn = MySQLConnect('localhost', 'root', 'yexinjing', 'dige')
    sql = '''
    
    '''
    conn.execute(sql)

if __name__ == '__main__':
    stat_data()
