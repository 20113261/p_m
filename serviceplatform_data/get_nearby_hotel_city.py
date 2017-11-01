#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 下午2:41
# @Author  : Hou Rong
# @Site    : 
# @File    : get_nearby_hotel_city.py
# @Software: PyCharm
import pymongo
from pymysql.cursors import DictCursor
from logger import get_logger
from service_platform_conn_pool import private_data_test_pool, source_info_pool, task_db_spider_db_pool, base_data_pool

logger = get_logger("get_nearest_source_and_city")

client = pymongo.MongoClient(host='10.10.231.105', port=27017)
db = client['HotelData']
collections = client['HotelData']['detail_final_data']
city_collections = client['HotelData']['city']


def get_per_nearby_city(city_map_info, distance=50):
    """
    获取附近酒店的 source、sid
    :param distance: city hotel distance , default 50km
    :param city_map_info:
    :return:
    """
    lon, lat = city_map_info.split(',')

    for each in collections.aggregate(
            [
                {
                    "$match": {"loc":
                        {
                            "$geoWithin": {
                                "$centerSphere": [[float(lon), float(lat)],
                                                  distance / 6378.1]
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {"source": "$source", "source_city_id": "$source_city_id"},
                        "count": {"$sum": 1}
                    }
                }
            ]
    ):
        source = each['_id']['source']
        source_city_id = each['_id']['source_city_id']
        count = each["count"]
        logger.debug("[get city][source: {}][source_city_id: {}][count: {}]".format(source, source_city_id, count))
        yield source, source_city_id, count


def get_old_task_info_set(mioji_cids):
    old_task_info_set = set()
    conn = source_info_pool.connection()
    cursor = conn.cursor()
    if not mioji_cids:
        return old_task_info_set
    sql = '''SELECT source,sid
FROM hotel_suggestions_city_new
WHERE city_id IN ({}) AND source IN ('booking', 'ctrip', 'agoda', 'expedia', 'hotels', 'elong');'''.format(
        ','.join(mioji_cids))
    cursor.execute(sql)
    for source, sid in cursor.fetchall():
        if sid:
            old_task_info_set.add((source, sid))
    logger.debug("[mioji_cids: {}][task count: {}]".format(mioji_cids, len(old_task_info_set)))
    cursor.close()
    conn.close()
    return old_task_info_set


def get_nearby_mioji_city(city_map_info):
    """
    搜索 50km 內的 mioji 城市 id
    :param city_map_info:
    :return:
    """
    lon, lat = city_map_info.split(',')
    res = city_collections.distinct('id',
                                    {
                                        "loc":
                                            {
                                                "$geoWithin":
                                                    {
                                                        "$centerSphere": [[float(lon), float(lat)], 50 / 6378.1]
                                                    }
                                            }
                                    }
                                    )
    logger.debug("[city map info: {}][count: {}]".format(city_map_info, len(res)))
    return res


def insert_db(data):
    conn = task_db_spider_db_pool.connection()
    cursor = conn.cursor()
    query_sql = '''REPLACE INTO private_city_task (id, name, name_en, map_info, source, sid, is_private_city, search_kilometer, `count`)
VALUES (%(id)s, %(name)s, %(name_en)s, %(map_info)s, %(source)s, %(sid)s, %(is_private_city)s, %(search_kilometer)s, %(count)s);'''
    _res = cursor.executemany(query_sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[insert data][total: {}][execute: {}]".format(len(data), _res))


def is_map_info_legal(map_info):
    try:
        lon, lat = map_info.split(',')
        float(lon)
        float(lat)
        return True
    except Exception as exc:
        logger.exception(msg="[map info illegal][map_info: {}]".format(map_info), exc_info=exc)
        return False


def city_task():
    # private city 30km
    conn = private_data_test_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute('''SELECT
      id,
      name,
      name_en,
      map_info
    FROM city;''')
    for line in cursor.fetchall():
        if is_map_info_legal(line['map_info']):
            line['search_kilometer'] = 30
            line['is_private_city'] = True
            yield line
            line['search_kilometer'] = 50
            line['is_private_city'] = True
            yield line
    cursor.close()
    conn.close()

    conn = base_data_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute('''SELECT
      id,
      name,
      name_en,
      map_info
    FROM city;''')
    for line in cursor.fetchall():
        if is_map_info_legal(line['map_info']):
            line['search_kilometer'] = 30
            line['is_private_city'] = False
            yield line
            line['search_kilometer'] = 50
            line['is_private_city'] = False
            yield line
    cursor.close()
    conn.close()


def get_nearby_city():
    data = []
    for each in city_task():
        has_hotel = False
        for source, sid, _detail_count in get_per_nearby_city(each["map_info"], each["search_kilometer"]):
            has_hotel = True
            data.append({
                "id": each["id"],
                "name": each["name"],
                "name_en": each["name_en"],
                "map_info": each["map_info"],
                "is_private_city": each["is_private_city"],
                "source": source,
                "sid": sid,
                "search_kilometer": each["search_kilometer"],
                "count": _detail_count,
            })
            if len(data) == 1000:
                insert_db(data)
                data = []
            logger.debug(
                "[private city][id: {}][name: {}][name_en: {}][map_info: {}][is_private_city: {}][search_kilometer: {}]"
                "[source: {}][sid: {}]".format(
                    each["id"], each["name"], each["name_en"], each["map_info"], each["is_private_city"],
                    each["search_kilometer"], source, sid))
        if not has_hotel:
            logger.debug(
                "[no hotel nearby][id: {}][name: {}][name_en: {}][map_info: {}]".format(
                    each["id"], each["name"], each["name_en"], each["map_info"]))
    if data:
        insert_db(data)


if __name__ == '__main__':
    get_nearby_city()
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
