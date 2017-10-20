#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 下午2:41
# @Author  : Hou Rong
# @Site    : 
# @File    : get_nearby_hotel_city.py
# @Software: PyCharm
import pymongo
import dataset
from pymysql.cursors import DictCursor
from logger import get_logger
from service_platform_conn_pool import private_data_test_pool

logger = get_logger("get_nearest_source_and_city")

client = pymongo.MongoClient(host='10.10.231.105', port=27017)
db = client['HotelData']
collections = client['HotelData']['detail_final_data']


def get_per_nearby_city(city_map_info):
    """
    50km 的酒店 source、sid
    :param city_map_info:
    :return:
    """
    lon, lat = city_map_info.split(',')
    for each in collections.aggregate(
            [
                {"$match": {"loc":
                    {
                        "$geoNear": {"$geometry": {
                            "type": "Point",
                            "coordinates": [float(lon), float(lat)]
                        },
                            "$maxDistance": 50000}
                    },
                }},
                {"$group": {"_id": {"source": "$source", "source_city_id": "$source_city_id"}}}
            ]
    ):
        source = each['_id']['source']
        source_city_id = each['_id']['source_city_id']
        logger.debug("[get city][source: {}][source_city_id: {}]".format(source, source_city_id))
        yield source, source_city_id


def get_has_nearby_city(city_map_info):
    """
    50km 是否有城市
    :param city_map_info:
    :return:
    """
    lon, lat = city_map_info.split(',')
    res = collections.count({"loc": {
        "$geoNear": {"$geometry": {
            "type": "Point",
            "coordinates": [float(lon), float(lat)]
        },
            "$maxDistance": 50000}
    }})
    return bool(res)


def get_nearby_city():
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.238.148/spider_db?charset=utf8')
    table = db["private_city_task"]
    conn = private_data_test_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute('''SELECT
  id,
  name,
  name_en,
  map_info
FROM city;''')
    for each in cursor.fetchall():
        has_nearby_city = get_has_nearby_city(each["map_info"])
        for source, sid in get_per_nearby_city(each["map_info"]):
            data = {
                "id": each["id"],
                "name": each["name"],
                "name_en": each["name_en"],
                "map_info": each["map_info"],
                "has_nearby_city": has_nearby_city,
                "source": source,
                "sid": sid
            }
            table.upsert(data, keys=['id', 'source', 'sid'])
            logger.debug(
                "[private city][id: {}][name: {}][name_en: {}][map_info: {}][has_nearby_city: {}][source: {}][sid: {}]".format(
                    each["id"], each["name"], each["name_en"], each["map_info"], has_nearby_city, source, sid))
        db.commit()


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
