#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/2 下午9:30
# @Author  : Hou Rong
# @Site    : 
# @File    : attr.py
# @Software: PyCharm
import pandas as pd, numpy as np
from sqlalchemy import create_engine
import csv
import json
import re
import time
import datetime
from pymysql.cursors import DictCursor
from collections import defaultdict

record_file_name = 'record.csv'
conn_cx = create_engine('mysql+pymysql://chenxiang:chenxiang@10.10.180.145/chenxiang')
conn_rd = create_engine('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data')
a = 0


def log_normal(*args):
    print('\033[1;32;m', end='')
    print('[*****', *args, '******][{}]'.format(datetime.datetime.now()), end='')
    print('\033[0m', )


def log_error(*strs):
    print('\033[1;31;m', end='')
    print('[*****', *strs, '******][{}]'.format(datetime.datetime.now()), end='')
    print('\033[0m', )


def write_file(record_file_name, id, source_id, source, code, info):
    with open(record_file_name, 'a+') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([id, source_id, source, code, info])


def satisfied_data(row, attr, attr_unid):
    unid_satisfi = attr_unid[row['id']]
    if not len(unid_satisfi):
        log_error('unid~ID未找到-1！！')
        write_file(record_file_name, row['id'], '', '', '-1', '-1')
        return

    for source, source_id in unid_satisfi:
        attr_satisfi = attr[(source, source_id)]

        if not len(attr_satisfi):
            log_error('attr~source未找到-2！')
            write_file(record_file_name, row['id'], source_id, '', '-2', '-1')
            continue
        attr_satisfi = attr_satisfi[0]

        # No.1 match try
        if row['city_id'] == attr_satisfi['city_id']:
            attr_alias = '' if attr_satisfi['alias'] in ['NULL', '', '0', 'None', np.nan] else attr_satisfi['alias']
            name_chat_set = {row['name'], row['name_en']} | set(row['alias'].split('|'))
            name_attr_set = {attr_satisfi['name'], attr_satisfi['name_en']} | set(attr_alias.split('|'))
            commn = name_attr_set & name_chat_set - {''}
            if commn:
                log_normal("匹配正确10:", commn)
                write_file(record_file_name, row['id'], source_id, attr_satisfi['source'], 10, commn)
                continue

        # No.2 match try
        url_attr = str(attr_satisfi['url']).strip('https:/')
        chat_url = json.loads(row['url'])
        if url_attr in map(lambda x: str(x).strip('https:/'), chat_url.values()):
            write_file(record_file_name, row['id'], source_id, attr_satisfi['source'], 20, 'url')
            continue
        else:
            if 'tripadvisor' in url_attr:
                daodao_chat = chat_url.get('daodao')
                if daodao_chat:
                    try:
                        daodao_chat = re.search('-d(\d+)', daodao_chat).group(1)
                        daodao_attr = re.search('-d(\d+)', url_attr).group(1)
                    except:
                        pass
                    else:
                        if daodao_attr == daodao_chat:
                            log_normal('匹配正确21：', daodao_attr)
                            write_file(record_file_name, row['id'], source_id, attr_satisfi['source'], 21, daodao_attr)
                        continue
            elif 'qyer' in url_attr:
                qyer_chat = chat_url.get('qyer')
                if qyer_chat:
                    try:

                        qyer_chat = qyer_chat.split('/')
                        qyer_chat = qyer_chat[qyer_chat.index('poi') + 1]
                        qyer_attr = url_attr.split('/')
                        qyer_attr = qyer_attr[qyer_attr.index('poi') + 1]
                    except:
                        pass
                    else:
                        if qyer_attr == qyer_chat:
                            log_normal('匹配正确22：', qyer_attr)
                            write_file(record_file_name, row['id'], source_id, attr_satisfi['source'], 22, qyer_attr)
                            continue
        # No.3 match
        if row['add_info']:
            add_info = set(re.split('[| ]+', row['add_info']))
            attr_alias = '' if attr_satisfi['alias'] in ['NULL', '', '0', 'None', np.nan] else attr_satisfi['alias']
            name_attr_set = {attr_satisfi['name'], attr_satisfi['name_en']} | set(attr_alias.split('|'))
            info_c = add_info & name_attr_set - {''}
            if info_c:
                log_normal('匹配正确30:', info_c)
                write_file(record_file_name, row['id'], source_id, attr_satisfi['source'], 30, info_c)
                continue

        log_error('没有匹配0！！！！')
        write_file(record_file_name, row['id'], source_id, attr_satisfi['source'], 0, 0)


if __name__ == '__main__':
    import pymysql

    with open(record_file_name, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['id', 'source_id', 'source', 'code', 'info'])

    log_normal('read chat_attraction...')
    chat_attraction = pd.read_sql_table('chat_attraction', conn_rd)

    # attr = pd.read_sql_table('attr', conn_cx)
    # attr = attr.fillna('')
    # attr_unid = pd.read_sql_table('attr_unid', conn_cx)
    # attr_unid = attr_unid.fillna('')
    record_file_name = 'record.csv'
    log_normal('read attr...')
    conn_cx = create_engine('mysql+pymysql://chenxiang:chenxiang@10.10.180.145/chenxiang')
    conn_rd = create_engine('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data')
    attr_conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8',
                                db='chenxiang')
    attr_cursor = attr_conn.cursor(cursor=DictCursor)
    attr_cursor.execute('''SELECT
  ifnull(id, '') as id,
  ifnull(source, '') as source,
  ifnull(name, '') as name,
  ifnull(name_en, '') as name_en,
  ifnull(alias, '') as alias,
  ifnull(city_id, '') as city_id,
  ifnull(url, '') as url
FROM attr;''')

    attr = defaultdict(list)
    for line in attr_cursor.fetchall():
        attr[(line['source'], line['id'])].append({k: v or '' for k, v in line.items()})

    log_normal('read attr_unid')
    attr_unid_conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8',
                                     db='chenxiang')
    attr_unid_cursor = attr_unid_conn.cursor(cursor=DictCursor)
    attr_unid_cursor.execute('select id,source,source_id from attr_unid')

    attr_unid = defaultdict(list)
    for line in attr_unid_cursor.fetchall():
        attr_unid[line['id']].append((line['source'], line['source_id']))

    print('Func Start')


    def sat(row):
        satisfied_data(row, attr, attr_unid)


    log_normal('mapping...')
    chat_attraction.apply(sat, axis=1)
