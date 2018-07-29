#!/usr/bin/env python2
# coding: utf-8

import re
import json
from datetime import datetime
import tushare as ts
from sqlalchemy import create_engine
from SQLConnector import load_to_database, MySQLConnect
from webscraping import download, xpath, common, adt

def load_code2name(conn):
    load_to_database('code_to_name', './stocks.csv', db='dige', delete=False)
    sql = 'update dige.code_to_name set update_time=now()'
    conn.execute(sql)

def init_nocsi(code):
    engine = create_engine('mysql://root:yexinjing@localhost:3306/dige', echo=False)
    try:
        df = ts.get_k_data(code, ktype='D', index=True, start='2015-07-25')
        df.to_sql('quote_nocsi', engine, if_exists='append')
    except Exception, e:
        print e

def init_csi():
    data = []
    f = open('datacsi.csv', 'w')
    for i in open('csi.csv'):
        for k in json.loads(i.strip()):
            tradedate = k['tradedate'].split(' ')[0].replace('-', '')
            string = '\t'.join([k['indx_code'], k['tclose'], tradedate])
            f.write(string + '\n')
    f.close()
    load_to_database('quote_csi', './datacsi.csv', db='dige', delete=False)

def init_database(conn):
    csi = []
    nocsi = []
    with open('stocks.csv', 'r') as f:
        for i in f:
            code = i.split('\t')[0]
            if 'CSI' not in i:
                init_nocsi(code)

def incr_database(conn):
    # csi
    D = download.Download(delay=0, read_cache=None, write_cache=None)
    data = []
    csi = []
    src = 'http://www.csindex.com.cn/zh-CN/indices/index-detail/'
    for i in open('stocks.csv'):
        code = i.split('\t')[0]
        if 'CSI' in i or '000985' in i:
            url = src + code
            html = D.get(url)
            trddate = common.regex_get(html, r'截止日期:([^<]+)<')
            if trddate:
                trddate = trddate.replace('-', '')
            m = xpath.search(html, r'//table[@class="table\stc"]/tr/td', remove=None)
            close = m[0] if m else None
            sql = ''' 
                     REPLACE INTO quote_csi(code, close, date) VALUES('%s',%s,%s);
            ''' % (code, close, trddate)
            conn.execute(sql)
        else:
            today = datetime.today().strftime('%Y-%m-%d')
            engine = create_engine('mysql://root:yexinjing@localhost:3306/dige', echo=False)
            try:
                df = ts.get_k_data(code, ktype='D', index=True, start=today, end=today)
                if not df.empty:
                    sql = ''' delete from quote_nocsi where code like '%%%s%%' and date = '%s' ''' % (code, today)
                    conn.execute(sql)
                    df.to_sql('quote_nocsi', engine, if_exists='append')
            except Exception, e:
                print e

def scraper_from_tushare(conn):
    # 初始化数据库
    #init_database(conn)
    # 增量更新数据库
    incr_database(conn)

if __name__ == '__main__':
    conn = MySQLConnect('localhost', 'root', 'yexinjing', 'dige')
    try:
        scraper_from_tushare(conn)
        #init_csi()
    finally:
        conn.close()
