#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/22 下午4:25
# @Author  : Hou Rong
# @Site    : 
# @File    : share_station.py
# @Software: PyCharm
from warnings import filterwarnings

import numpy as np
import pymysql
from service_platform_conn_pool import fetchall, new_station_pool
from toolbox.Common import is_legal
from logger import get_logger

filterwarnings('ignore', category=pymysql.err.Warning)
logger = get_logger("shared_station")

STATION_SRC_TABLE = 'station_src'
STATION_RELATION_TABLE = 'station_relation'


def dist_from_coordinates(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km

    # conversion to radians
    d_lat = np.radians(lat2 - lat1)
    d_lon = np.radians(lon2 - lon1)

    r_lat1 = np.radians(lat1)
    r_lat2 = np.radians(lat2)

    # haversine formula
    a = np.sin(d_lat / 2.) ** 2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(d_lon / 2.) ** 2

    haversine = 2 * R * np.arcsin(np.sqrt(a))

    return haversine


def get_distance(c_map_info, p_map_info):
    try:
        c_lon, c_lat = c_map_info.split(',')
        p_lon, p_lat = p_map_info.split(',')
        try:
            distance = float(dist_from_coordinates(float(c_lat), float(c_lon), float(p_lat), float(p_lon)))
            # logger.info('[c_map_info: {}][p_map_info: {}][distance: {}]'.format(c_map_info, p_map_info, distance))
            return distance
        except Exception as exc:
            logger.exception("[error distance calc][c_map_info: {}][p_map_info: {}]".format(c_map_info, p_map_info),
                             exc_info=exc)
    except:
        pass
    return None


def insert_db(data, multi=True):
    sql = '''INSERT IGNORE INTO {} (station_id, map_info, city_id, city_map_info, distance, info)
VALUES (%s, %s, %s, %s, %s, %s);'''.format(STATION_RELATION_TABLE)
    conn = new_station_pool.connection()
    cursor = conn.cursor()
    try:
        if multi:
            res = cursor.executemany(sql, data)
            logger.info("[insert data][count: {}][insert: {}]".format(len(data), res))
        else:
            cursor.execute(sql, data)
            logger.info("[insert data][data: {}]".format(data))
    except Exception as exc:
        logger.exception("[insert data exc][sql: {}][data: {}]".format(sql, data), exc_info=exc)
    conn.commit()
    cursor.close()
    conn.close()


def belong_city_id_insert():
    """
    通过 belong_city_id 获取 city_id
    :return:
    """
    sql = '''SELECT
  station_id,
  {0}.map_info              AS station_map_info,
  {0}.station_city_map_info AS station_city_map_info,
  city.id                               AS city_id,
  city.map_info                         AS city_map_info
FROM {0}
  JOIN base_data.city ON {0}.belong_city_id = base_data.city.id;'''.format(STATION_SRC_TABLE)
    _count = 0
    data = []
    for station_id, station_map_info, station_city_map_info, city_id, city_map_info in fetchall(new_station_pool, sql):
        _count += 1
        if is_legal(station_map_info):
            distance = get_distance(city_map_info, station_map_info)
            map_info = station_map_info
        elif is_legal(station_city_map_info):
            distance = get_distance(city_map_info, station_map_info)
            map_info = station_city_map_info
        else:
            distance = None
            map_info = None
        '''
        station_id, map_info, city_id, city_map_info, distance, info
        '''
        data.append((
            station_id,
            map_info,
            city_id,
            city_map_info,
            distance,
            '通过 belong_city_id 进行匹配'
        ))
        if len(data) == 1000:
            insert_db(data)
            data = []

    if data:
        insert_db(data)


def get_city_station_list():
    city_sql = '''SELECT base_data.city.id,
      base_data.city.map_info
        FROM base_data.city
          JOIN base_data.country ON base_data.city.country_id = base_data.country.mid
          JOIN base_data.continent ON base_data.country.continent_id = base_data.continent.id
        WHERE base_data.continent.name = '欧洲' AND base_data.city.id NOT IN (SELECT city_id
FROM {0}
WHERE info != '旧数据保留';);'''.\
        format(STATION_RELATION_TABLE)

    station_sql = '''SELECT
      station_id,
      map_info,
      station_city_map_info
    FROM {0}
    WHERE utime > '2017-11-21';'''.format(STATION_SRC_TABLE)

    city_list = list(fetchall(new_station_pool, city_sql))
    station_list = list(fetchall(new_station_pool, station_sql))
    return city_list, station_list


def shared_city_id_insert():
    city_list, station_list = get_city_station_list()
    data = []
    _count = 0
    near_count = 0
    far_count = 0
    no_count = 0
    no_set = set()
    no_map_info_count = 0
    no_map_info_set = set()
    for city_id, city_map_info in city_list:
        _count += 1
        if is_legal(city_map_info):
            near_station = []
            far_station = []
            for station_id, station_map_info, station_city_map_info in station_list:
                if is_legal(station_map_info):
                    distance = get_distance(city_map_info, station_map_info)
                    map_info = station_map_info
                elif is_legal(station_city_map_info):
                    distance = get_distance(city_map_info, station_map_info)
                    map_info = station_city_map_info
                else:
                    distance = None
                    map_info = None
                if distance is None:
                    continue
                elif distance < 20:
                    near_station.append((station_id, map_info, distance))
                elif distance < 50:
                    far_station.append((station_id, map_info, distance))

            near_station = sorted(near_station, key=lambda x: x[-1])
            far_station = sorted(far_station, key=lambda x: x[-1])

            if near_station:
                new_near_station = near_station[:3]
            else:
                new_near_station = []
            if far_station:
                new_far_station = far_station[0]
            else:
                new_far_station = []

            if new_near_station:
                for station_id, map_info, distance in new_near_station:
                    data.append(
                        (
                            station_id,
                            map_info,
                            city_id,
                            city_map_info,
                            distance,
                            '20km 匹配 3 条'
                        )
                    )
                near_count += 1
                logger.info("[20 匹配][count: {}][near: {}][city_id: {}][station: {}]".
                            format(_count, near_count, city_id, new_near_station))
            elif new_far_station:
                station_id, map_info, distance = new_far_station
                data.append(
                    (
                        station_id,
                        map_info,
                        city_id,
                        city_map_info,
                        distance,
                        '50km 匹配 1 条'
                    )
                )
                far_count += 1
                logger.info("[50 匹配][count: {}][far: {}][city_id: {}][station: {}]".
                            format(_count, far_count, city_id, new_far_station))
            else:
                no_count += 1
                logger.info("[无 station 城市][count: {}][no: {}][city_id: {}]".
                            format(_count, no_count, city_id))
                no_set.add(city_id)
                continue
        else:
            logger.info(
                "[无 map_info 城市][count: {}][no_map_info: {}][city_id: {}]".
                    format(_count, no_map_info_count, city_id))
            no_map_info_set.add(city_id)
            continue

        if len(data):
            insert_db(data)
        # if len(data) > 1:
        #     insert_db(data)
        # elif len(data) == 1:
        #     insert_db(data, False)
        data = []

    logger.info(
        "[near_count: {}][far_count: {}][no_count: {}][no_map_info_count: {}]".format(near_count, far_count, no_count,
                                                                                      no_map_info_count))
    logger.info("[no_set: {}]".format(no_set))
    logger.info("[no_map_info_set: {}]".format(no_map_info_set))


def get_info():
    sql = '''SELECT base_data.city.*
FROM base_data.city
  JOIN base_data.country ON base_data.city.country_id = base_data.country.mid
  JOIN base_data.continent ON base_data.country.continent_id = base_data.continent.id
WHERE base_data.continent.name = '欧洲' AND base_data.city.id NOT IN (SELECT city_id
                                                                    FROM station_relation_new);'''
    for line in fetchall(new_station_pool, sql):
        pass


if __name__ == '__main__':
    belong_city_id_insert()
    shared_city_id_insert()
