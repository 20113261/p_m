#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/15 下午11:05
# @Author  : Hou Rong
# @Site    : 
# @File    : baidu_sugg.py
# @Software: PyCharm
from warnings import filterwarnings

import pymongo
import pymysql
import json
import re
from urllib.parse import urljoin
from toolbox.Hash import encode

filter
warnings('ignore', category=pymysql.err.Warning)

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


def qyer_baidu_city():
    results = db.BaiDuSuggest.find({})
    conn = pymysql.connect(**mysql_config)
    cursor = conn.cursor()
    insert_sql = "insert ignore into ota_location_qyer_1215(source,sid_md5,sid,suggest_type,suggest,city_id,country_id,s_city,s_region,s_country,s_extra,label_batch,others_info) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    results_list = []
    _count = 0
    for result in results:
        _count += 1
        city_list = result.get('city_url')
        for city in city_list:
            if 'poi' not in city:
                try:
                    hotel_url = city.replace('///', '//')
                    sid = re.search(r'place\.qyer\.com/(.*?)(?=/)', hotel_url).group(1)
                    hotel_url = 'http://place.qyer.com/{0}/'.format(sid)
                    city_name = sid
                    others_info = {'from': 'baidu_suggest'}
                    others_info = json.dumps(others_info)
                    results_list.append(
                        ('qyer', encode(sid), sid, 1, hotel_url, 'NULL', 'NULL', city_name, 'NULL', 'NULL', 'NULL',
                         '2017-12-13a', others_info)
                    )
                    # print('qyer', sid, 1, hotel_url, 'NULL', 'NULL', city_name, 'NULL', 'NULL', 'NULL', '2017-12-13a',
                    #       others_info)
                    if len(results_list) >= 2000:
                        print('*' * 10, _count, '*' * 10)
                        cursor.executemany(insert_sql, results_list)
                        conn.commit()
                        results_list = []
                except Exception as e:
                    pass
    else:
        cursor.executemany(insert_sql, results)
        conn.commit()
    print('*' * 100, _count, '*' * 100)


if __name__ == "__main__":
    qyer_baidu_city()
