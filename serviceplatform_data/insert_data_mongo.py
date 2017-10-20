#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/19 下午7:48
# @Author  : Hou Rong
# @Site    :
# @File    : insert_data_mongo.py
# @Software: PyCharm
import pymongo
from pymysql.cursors import DictCursor
from pymongo.errors import BulkWriteError
from data_source import MysqlSource
from logger import get_logger
from service_platform_conn_pool import service_platform_pool, base_data_pool

logger = get_logger("insert_mongo_data")

data_db = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'ServicePlatform'
}

client = pymongo.MongoClient(host='10.10.231.105', port=27017)
hotel_collections = client['HotelData']['detail_final_data']
city_collections = client['HotelData']['city']


def insert_hotel_data():
    logger.debug("start prepare mongo data")
    logger.debug("get all table name")

    local_conn = service_platform_pool.connection()
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'ServicePlatform';''')

    # 强制要求按照 tag 的先后顺序排列
    table_list = list(
        sorted(
            filter(lambda x: x.startswith('detail_hotel_') and x.split('_')[-1] != "test" and x.split("_")[
                                                                                                  -1] > "20170928d",
                   map(lambda x: x[0],
                       local_cursor.fetchall()
                       )
                   ),
            key=lambda x: x.split('_')[-2:]
        )
    )
    local_cursor.close()
    local_conn.close()

    for each_table_name in table_list:
        _count = 0
        data = []
        delete_sid = []
        delete_source = ""
        sql = '''SELECT
          source,
          source_id,
          hotel_name,
          hotel_name_en,
          map_info,
          address,
          source_city_id
        FROM {};'''.format(each_table_name)
        for each_data in MysqlSource(data_db, table_or_query=sql, size=10000,
                                     is_table=False, is_dict_cursor=True):
            map_info = each_data['map_info']
            lng, lat = map_info.split(',')
            each_data["loc"] = {"type": "Point", "coordinates": [float(lng), float(lat)]}
            data.append(each_data)

            delete_source = each_data["source"]
            delete_sid.append(each_data["source_id"])

            _count += 1
            if _count % 10000 == 0:
                try:
                    hotel_collections.delete_many({"source": delete_source, "source_id": {"$in": delete_sid}})
                    hotel_collections.insert_many(data)
                except BulkWriteError as bwe:
                    logger.exception(msg="[bwe error][bwe details: {}]".format(bwe.details))
                except Exception as exc:
                    logger.exception(msg="[insert data error]", exc_info=exc)
                data = []
                delete_sid = []
                logger.debug("[insert_data][table: {}][count: {}]".format(each_table_name, _count))

        if data:
            try:
                hotel_collections.delete_many({"source": delete_source, "source_id": {"$in": delete_sid}})
                hotel_collections.insert_many(data)
            except BulkWriteError as bwe:
                logger.exception(msg="[bwe error][bwe details: {}]".format(bwe.details))
            except Exception as exc:
                logger.exception(msg="[insert data error]", exc_info=exc)
        logger.debug("[insert_data][table: {}][count: {}]".format(each_table_name, _count))


def insert_city_data():
    sql = '''SELECT
  id,
  name,
  name_en,
  map_info
FROM city;'''
    conn = base_data_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    cursor.close()
    conn.close()

    _count = 0
    _total = 0
    for each in cursor.fetchall():
        _total += 1
        try:
            map_info = each['map_info']
            lng, lat = map_info.split(',')
            each["loc"] = {"type": "Point", "coordinates": [float(lng), float(lat)]}
        except Exception as e:
            logger.exception(msg="[map info error]", exc_info=e)
            continue
        city_collections.update({"id": each["id"]}, each, upsert=True)
        _count += 1
    logger.debug("[insert_data][table: city][total: {}][count: {}]".format(_total, _count))


if __name__ == '__main__':
    insert_hotel_data()
    insert_city_data()
