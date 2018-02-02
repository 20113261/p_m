#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/23 下午2:54
# @Author  : Hou Rong
# @Site    : 
# @File    : nearby_city_task_count.py
# @Software: PyCharm
import pymongo
from pymysql.cursors import DictCursor
from my_logger import get_logger
from service_platform_conn_pool import private_data_test_pool, source_info_pool

logger = get_logger("nearby_city_report")

client = pymongo.MongoClient(host='10.10.231.105', port=27017)
db = client['HotelData']
collections = client['HotelData']['city']


def get_old_task_count(mioji_cids):
    conn = source_info_pool.connection()
    cursor = conn.cursor()
    if not mioji_cids:
        return 0
    sql = '''SELECT count(*)
FROM hotel_suggestions_city
WHERE city_id IN ({}) AND source IN ('booking', 'ctrip', 'agoda', 'expedia', 'hotels', 'elong');'''.format(
        ','.join(mioji_cids))
    cursor.execute(sql)
    _count = cursor.fetchone()[0]
    logger.debug("[mioji_cids: {}][task count: {}]".format(mioji_cids, _count))
    cursor.close()
    conn.close()
    return _count


def get_nearby_mioji_city(city_map_info):
    """
    50km 是否有城市
    :param city_map_info:
    :return:
    """
    lon, lat = city_map_info.split(',')
    res = collections.distinct('id',
                               {
                                   "loc":
                                       {
                                           "$geoWithin":
                                               {
                                                   "$centerSphere": [[float(lon), float(lat)], 300 / 6378.1]
                                               }
                                       }
                               }
                               )
    logger.debug("[city map info: {}][count: {}]".format(city_map_info, len(res)))
    return res


def get_nearby_city_task_report():
    conn = private_data_test_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute('''SELECT
  id,
  name,
  name_en,
  map_info
FROM city;''')
    __count = 0
    for each in cursor.fetchall():
        nearby_city = get_nearby_mioji_city(each["map_info"])
        _count = get_old_task_count(nearby_city)
        logger.debug(
            "[private city][id: {}][name: {}][name_en: {}][map_info: {}][total count: {}][old task count: {}]".format(
                each["id"],
                each["name"],
                each["name_en"],
                each["map_info"],
                __count,
                _count))
        __count += _count
    logger.debug("[old task total][count: {}]".format(__count))


if __name__ == '__main__':
    get_nearby_city_task_report()
    '''
    # Mongo 获取带经纬度距离的方法
     result = db.command({"geoNear": "detail_final_data",
                         "near": {"type": "Point", "coordinates": [2.351492339147967, 48.85746107178952]},
                         "spherical": True,
                         "maxDistance": 50000})
    _count = 0
    for each in result["results"]:
        print(each)

    print(_count)
    '''
