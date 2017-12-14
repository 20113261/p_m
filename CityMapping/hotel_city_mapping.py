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
from service_platform_conn_pool import base_data_pool, source_info_str, source_info_pool, fetchall
from logger import get_logger

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


def hotels_get_geo_id_by_dest_id(dest_id):
    sql = '''SELECT sid
FROM ota_location
WHERE source = 'hotels' AND suggest_type = 2 AND json_extract(suggest, '$.destinationId') = '{}';'''.format(dest_id)
    res = None
    for line in fetchall(source_info_pool, sql):
        res = line

    if res:
        return res[0]
    else:
        return None


if __name__ == '__main__':
    # city_id_key = 'id'
    city_id_key = 'id'
    source = 'expedia'
    label = '2017-12-14a'
    db = dataset.connect(source_info_str)
    table = db['ota_location']
    # pandas_table = pandas.read_csv('/Users/hourong/Downloads/1206_city/酒店-表格 1.csv')
    pandas_table = pandas.read_csv('/tmp/酒店-表格 1.csv')
    pandas_table.fillna('NULL', inplace=True)
    for i in range(len(pandas_table)):
        line = pandas_table.iloc[i]
        city_id = str(line[city_id_key])
        if source == 'daodao':
            suggest = line['猫途鹰链接']
            _l_sid = re.findall('-(g\d+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                continue
        elif source == 'qyer':
            suggest = line['穷游链接']
            _l_sid = re.findall('http://place.qyer.com/(\S+?)/', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                continue
        elif source == 'agoda':
            suggest = line[source]
            _l_sid = re.findall('city=(\d+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                # _l_sid = re.findall('area=(\d+)', suggest)
                # if _l_sid:
                #     sid = "area-{}".format(_l_sid[0])
                # else:
                #     _l_sid = re.findall('poi=(\d+)', suggest)
                #     if _l_sid:
                #         sid = "poi-{}".format(_l_sid[0])
                #     else:
                #         logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                continue
        elif source == 'elong':
            suggest = line[source]
            if 'poi_' in suggest:
                poi_id = re.findall('poi_(\d+)', suggest)[0]
            else:
                poi_id = None

            _l_sid = re.findall('region_(\d+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
                if poi_id:
                    sid = '{}&{}'.format(sid, poi_id)
            else:
                logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                continue
        elif source == 'ctrip':
            suggest = line[source]
            _l_sid = re.findall('http://hotels.ctrip.com/international/([\s\S]+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                continue
        elif source == 'booking':
            suggest = line[source]
            _l_sid = re.findall('dest_id=([-\d]+)&', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                continue
        elif source == 'expedia':
            suggest = line[source]
            _l_sid = re.findall('regionId=([\d]+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                sid = 'city-{}'.format(city_id)
                logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                # continue
        elif source == 'hotels':
            suggest = line[source]
            _l_sid = re.findall('regionId=([\d]+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
            else:
                _l_sid = re.findall('destination-id=([\d]+)', suggest)
                if _l_sid:
                    dest_id = _l_sid[0]
                    sid = hotels_get_geo_id_by_dest_id(dest_id=dest_id)
                    if not sid:
                        sid = 'dest-{}'.format(dest_id)
                        # logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                        # continue
                else:
                    logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
                    continue
                    # sid = 'geo-cid-{}'.format(city_id)
        else:
            continue
        res = table.find_one(source=source, sid=sid)
        # print(sid, suggest, res)
        if res:
            cid, country_id, city_name, country_name = city_info[city_id]
            # data = {'id': res['id'], 'city_id': city_id, 'country_id': country_id, 'label_batch': label,
            #         'suggest': suggest, 'suggest_type': '1', }
            # table.update(data, keys=['id'])

            data = {'id': res['id'], 'city_id': city_id}
            table.update(data, keys=['id'])
            logger.info("[update city info][data: {}]".format(data))
        else:
            cid, country_id, city_name, country_name = city_info[city_id]
            data = {
                'source': source,
                'sid': sid,
                'suggest_type': '1',
                'suggest': suggest,
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
