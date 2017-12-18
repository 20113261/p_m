#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/15 下午10:39
# @Author  : Hou Rong
# @Site    : 
# @File    : qyer_sugg.py
# @Software: PyCharm
from warnings import filterwarnings

import pymongo
import pymysql
import json
from urllib.parse import urljoin
from toolbox.Hash import encode

filterwarnings('ignore', category=pymysql.err.Warning)

mongo_config = {
    'host': '10.10.213.148'
}
mysql_config = {
    'host': '10.10.230.206',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'source_info',
    'charset': 'utf8'
}

client = pymongo.MongoClient(**mongo_config)
db = client['SuggestName']


def qyer_city():
    results = db.QyerCity.find({})
    conn = pymysql.connect(**mysql_config)
    cursor = conn.cursor()
    insert_sql = '''INSERT IGNORE INTO ota_location_qyer_1215 (SOURCE, sid_md5, sid, suggest_type, suggest, city_id, country_id, s_city, s_region, s_country, s_extra, label_batch, others_info)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

    data = []
    _count = 0
    for result in results:
        _count += 1
        if _count % 1000 == 0:
            print(_count)
        city_list = result.get('city')
        for city in city_list:
            if city.get('type_name') == 'city':
                hotel_url = city.get('url')
                city_name = city.get('cn_name')
                city_name = city_name.replace('<span class="cGreen">', '').replace('</span>', '')
                hotel_url = urljoin('http:', hotel_url)
                if hotel_url.endswith('/'):
                    sid = hotel_url.split('/')[-2]
                else:
                    sid = hotel_url.split('/')[-1]
                    hotel_url = hotel_url + '/'
            others_info = {'form': 'qyer_suggest'}
            others_info = json.dumps(others_info)
            results = (
                'qyer', encode(sid), sid, 1, hotel_url, 'NULL', 'NULL', city_name, 'NULL', 'NULL', 'NULL',
                '2017-12-13a',
                others_info)
            # print('qyer', encode(sid), sid, 1, hotel_url, 'NULL', 'NULL', city_name, 'NULL', 'NULL', 'NULL',
            #       '2017-12-13a')
            data.append(results)
            if len(data) == 1000:
                try:
                    cursor.executemany(insert_sql, data)
                    conn.commit()
                    data = []
                except Exception as e:
                    conn.rollback()

    try:
        cursor.executemany(insert_sql, results)
        conn.commit()
    except Exception as e:
        conn.rollback()


# def qyer_baidu_city():
#     results = db.BaiDuSuggest.find({})
#     conn = pymysql.connect(**mysql_config)
#     cursor = conn.cursor()
#     insert_sql = "insert ignore into ota_location_qyer_1213(source,sid,suggest_type,suggest,city_id,country_id,s_city,s_region,s_country,s_extra,label_batch,others_info) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#     for result in results:

if __name__ == "__main__":
    qyer_city()
