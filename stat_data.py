#!/usr/bin/env python2
# coding: utf-8

import re
import sys
import bisect
from datetime import datetime, timedelta
from SQLConnector import MySQLConnect, load_to_database

def trddays():
    return [int(i.strip()) for i in open('tradeday.txt')]

def date_sub(date, n):
    return (datetime(int(date[0:4]), int(date[4:6]), int(date[6:])) - timedelta(days=int(n))).strftime('%Y%m%d')

def trddate_sub(date, n):
    trds = trddays()
    indx = bisect.bisect_left(trds, int(date))
    print indx

    if int(date) in trds:
        if indx >= n-1:
            indx = indx - n + 1
            return str(trds[indx])
    if int(date) not in trds:
        if indx >= n:
            indx = indx - n
            return str(trds[indx])
    return None

def stat_data():
    # 统计当日、5日、20日等涨跌幅数据
    reload(sys)
    sys.setdefaultencoding('utf8')

    conn = MySQLConnect('localhost', 'root', 'yexinjing', 'dige')
    today = '20180727'
    trddate_5days = trddate_sub(today, 5)
    trddate_20days = trddate_sub(today, 20)
    trddate_60days = trddate_sub(today, 60)
    trddate_120days = trddate_sub(today, 120)
    trddate_250days = trddate_sub(today, 250)

    this_year = trddate_sub(today[:4] + '0101', 0)

    m = str(int(today[:4]) - 1)
    last_year = trddate_sub(m + '0101', 0)
    m = str(int(today[:4]) - 2)
    before_last_year = trddate_sub(m + '0101', 0)

    date_2years_ago = date_sub(today, 3*365)

    sql = '''  select * 
               from quote_csi a left join code_to_name b on a.code = b.code 
               where a.date >= $2yearsago
               union 
               select b.code,  close, replace(date, '-', '') as date, (close-open)/open as chg, 
                       b.* 
               from quote_nocsi a 
               left join code_to_name b 
               on substring(a.code, 3, length(a.code)) = b.code
               where replace(date, '-', '') >= $2yearsago
    '''
    sql = sql.replace('$2yearsago', date_2years_ago)
    result = conn.query(sql)
    info = []
    bag = {}
    for i in result:
        code = i[0]
        date = i[2]
        chg = i[3]
        close = i[1]
        name = i[6]
        cate = i[5]
        bag.setdefault(code, {})
        bag[code].setdefault(date, {})
        bag[code].setdefault('name', name)
        bag[code].setdefault('cate', cate)
        bag[code][date].setdefault('close', close)
        bag[code][date].setdefault('changes', chg)

    f = open('result.csv', 'w')
    for code in bag.keys():
        name = bag[code]['name']
        cate = bag[code]['cate']
        today_close = bag[code][today]['close']
        d1 = bag[code][today]['changes']
        d5 = (today_close - bag[code][trddate_5days]['close']) / bag[code][trddate_5days]['close']
        d20 = (today_close - bag[code][trddate_20days]['close']) / bag[code][trddate_20days]['close']
        d60 = (today_close - bag[code][trddate_60days]['close']) / bag[code][trddate_60days]['close']
        d120 = (today_close - bag[code][trddate_120days]['close']) / bag[code][trddate_120days]['close']
        d250 = (today_close - bag[code][trddate_250days]['close']) / bag[code][trddate_250days]['close']
        year_0 = (today_close - bag[code][this_year]['close']) / bag[code][this_year]['close']
        year_1 = (today_close - bag[code][last_year]['close']) / bag[code][last_year]['close']
        year_2 = (today_close - bag[code][before_last_year]['close']) / bag[code][before_last_year]['close']
        res = [code, name, str(d1), str(d5), str(d20), str(d60), \
                str(d120), str(d250), str(year_0), str(year_1), str(year_2), \
                cate, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        string = '\t'.join(res) + '\n'
        f.write(string)
    f.close()
    load_to_database('index_performance', './result.csv', db='dige', delete=False, truncate=True)

if __name__ == '__main__':
    stat_data()
