#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/23 上午11:29
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_city_mapping.py
# @Software: PyCharm
import re
import pandas
import dataset
from service_platform_conn_pool import base_data_pool, source_info_str
from my_logger import get_logger

logger = get_logger("city_mapping")

conn = base_data_pool.connection()
cursor = conn.cursor()
cursor.execute('''SELECT
  city.id      AS city_id,
  country.mid  AS country_id,
  city.name    AS city_name,
  country.name AS country_name
FROM city
  JOIN country ON city.country_id = country.mid;''')
city_info = {line[0]: line for line in cursor.fetchall()}
conn.close()

if __name__ == '__main__':
    label = '2017-11-23a'
    db = dataset.connect(source_info_str)
    table = db['ota_location']
    pandas_table = pandas.read_csv('/Users/hourong/Downloads/poi_mapping.csv')
    for i in range(len(pandas_table)):
        line = pandas_table.iloc[i]
        city_id = str(line['id'])
        sid = re.findall('-(g\d+)', line['猫途鹰链接'])[0]
        res = table.find_one(source='daodao', sid=sid)
        if res:
            cid, country_id, city_name, country_name = city_info[city_id]
            data = {'id': res['id'], 'city_id': city_id, 'country_id': country_id, 'label_batch': label}
            table.update(data, keys=['id'])
            logger.info("[update city info][data: {}]".format(data))
        else:
            cid, country_id, city_name, country_name = city_info[city_id]
            data = {
                'source': 'daodao',
                'sid': sid,
                'suggest_type': '1',
                'suggest': line['猫途鹰链接'],
                'city_id': city_id,
                'country_id': country_id,
                's_city': city_name,
                's_country': country_name,
                's_extra': 'NULL',
                'label_batch': label
            }
            table.insert(data)
            logger.info("[insert new suggest][data: {}]".format(data))
    db.commit()
