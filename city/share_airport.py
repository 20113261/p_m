#!/usr/bin/env python
# -*- coding:utf-8 -*-
import csv
import pymysql
import traceback
import numpy as np
from logger import get_logger
from service_platform_conn_pool import fetchall, base_data_pool
from city.config import base_path
config = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}
city_conn = pymysql.connect(**config)


def dist_from_coordinates(lat1, lon1, lat2, lon2):
    r = 6371  # Earth radius in km

    # conversion to radians
    d_lat = np.radians(lat2 - lat1)
    d_lon = np.radians(lon2 - lon1)

    r_lat1 = np.radians(lat1)
    r_lat2 = np.radians(lat2)

    # haversine formula
    a = np.sin(d_lat / 2.) ** 2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(d_lon / 2.) ** 2

    haversine = 2 * r * np.arcsin(np.sqrt(a))

    return haversine


def get_need_share_airport_city():
    # 选取可用的城市
    city_sql = """SELECT id FROM city WHERE (status_test = 'Open' OR status_online = 'Open') AND id NOT IN (SELECT DISTINCT city_id
                                                                      FROM airport
                                                                      WHERE status = 'Open');"""

    # 选取可用的机场
    airport_sql = """SELECT
                  airport.id          AS id,
                  airport.map_info    AS map_info,
                  airport.inner_order AS inner_order,
                  city.country_id     AS country_id
                FROM airport
                  JOIN city ON airport.belong_city_id = city.id
                WHERE
                  airport.status = 'Open' AND airport.name != '' AND airport.name_en != '' 
                  AND airport.city_id = airport.belong_city_id;"""

    citys = list(fetchall(base_data_pool, city_sql))
    airports = list(fetchall(base_data_pool, airport_sql))
    return citys, airports


def condition_judge_1(id, city_id, inner_value, trans_degree, inner_order, distance):
    cursor = city_conn.cursor()
    try:
        sql = "SELECT trans_degree FROM city WHERE id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("codition_judge_1数据查询出现错误", e)
        city_conn.rollback()
    temp_degree = result[0]
    if int(temp_degree) == -1:
        temp_degree = 100000
    if int(inner_value) == -1:
        inner_value = 100000
    try:
        if not trans_degree.get(temp_degree, None):
            trans_degree[temp_degree] = [id, distance]
        else:
            if distance <= trans_degree[temp_degree][1]:
                trans_degree[temp_degree] = [id, distance]

        if not inner_order.get(inner_value, None):
            inner_order[inner_value] = [id, distance]
        else:
            if distance <= inner_order[inner_value][1]:
                inner_order[inner_value] = [id, distance]
    except Exception as e:
        print("codition_judge_1函数出现错误", e)


def condition_judge_2(id, city_id, inner_value, trans_degree, inner_order, distance):
    cursor = city_conn.cursor()
    try:
        sql = "SELECT trans_degree FROM city WHERE id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("condition_judge_2数据查询出现错误", e)
        city_conn.rollback()

    temp_degree = result[0]
    if int(temp_degree) == -1:
        temp_degree = 100000
    if int(inner_value) == -1:
        inner_value = 100000
    try:
        if not trans_degree.get(temp_degree, None):
            trans_degree[temp_degree] = [id, distance]
        else:
            if distance <= trans_degree[temp_degree][1]:
                trans_degree[temp_degree] = [id, distance]

        if not inner_order.get(inner_value, None):
            inner_order[inner_value] = [id, distance]
        else:
            if distance <= inner_order[inner_value][1]:
                inner_order[inner_value] = [id, distance]
    except Exception as e:
        print("condition_judge_2函数出现错误", e)


def condition_judge_3(id, city_id, inner_value, trans_degree, inner_order, distance):
    cursor = city_conn.cursor()
    try:
        sql = "SELECT trans_degree FROM city WHERE id=%s;"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("codition_judge_3数据查询出现错误", e)
        city_conn.rollback()
    temp_degree = result[0]
    if int(temp_degree) == -1:
        temp_degree = 100000
    if int(inner_value) == -1:
        inner_value = 100000
    try:
        if not trans_degree.get(temp_degree, None):
            trans_degree[temp_degree] = [id, distance]
        else:
            if distance <= trans_degree[temp_degree][1]:
                trans_degree[temp_degree] = [id, distance]

        if not inner_order.get(inner_value, None):
            inner_order[inner_value] = [id, distance]
        else:
            if distance <= inner_order[inner_value][1]:
                inner_order[inner_value] = [id, distance]
    except Exception as e:
        print("codition_judge_3函数出现错误", e)


def write_csv(city_id, _id):
    cursor = city_conn.cursor()
    city_sql = "SELECT id,country_id,status_online,map_info FROM city WHERE id = %s"
    airport_sql = "SELECT id,map_info,name,name_en,belong_city_id FROM airport WHERE id =%s"
    try:
        cursor.execute(city_sql, (city_id,))
        city_result = cursor.fetchone()
        cursor.execute(airport_sql, (_id,))
        airport_result = cursor.fetchone()
        with open(base_path+'share_airport.csv', 'a+') as airport:
            writer = csv.writer(airport)
            writer.writerow((city_result[0], city_result[1], city_result[3], city_result[2], airport_result[0],
                             airport_result[2], airport_result[3], airport_result[1], airport_result[4]))
    except Exception as e:
        city_conn.rollback()
        print(e)


def write_city_list(result):
    cursor = city_conn.cursor()
    select_sql = "SELECT id,name FROM city WHERE id=%s"
    try:
        cursor.execute(select_sql, result)
        results = cursor.fetchone()
    except Exception as e:
        city_conn.rollback()
    with open(base_path+'city_list.csv', 'a+') as city_list:
        writer = csv.writer(city_list)
        writer.writerow(results)


def update_share_airport():
    logger = get_logger('share_airport')
    with open(base_path+'city_list.csv', 'w+') as city:
        writer = csv.writer(city)
        writer.writerow(('city_id', 'city_name'))
    with open(base_path+'share_airport.csv', 'w+') as city:
        writer = csv.writer(city)
        writer.writerow(('city_id', 'country_id', 'status_online', 'city_mapInfo', 'airport_id', 'airport_mapInfo',
                         'name', 'name_en', 'belong_city_id'))
    cursor = city_conn.cursor()
    citys, airports = get_need_share_airport_city()

    select_city = 'SELECT map_info,country_id FROM city WHERE id = %s'

    for result in citys:
        cond_trans_degree_1 = {}
        cond_inner_order_1 = {}
        cond_trans_degree_2 = {}
        cond_inner_order_2 = {}
        cond_trans_degree_3 = {}
        cond_inner_order_3 = {}
        try:
            cursor.execute(select_city, result)
            city_mapInfo = cursor.fetchone()
            if len(city_mapInfo[0].strip(',').split(',')) == 2:
                city_lng, city_lat = city_mapInfo[0].strip(',').split(',')
            else:
                write_city_list(result)
                continue

            condition_1 = 0
            condition_2 = 0
            condition_3 = 0
            print("need_airport_city:", result)
            for open_airport in airports:
                if open_airport[3] == city_mapInfo[1]:
                    if len(open_airport[1].strip(',').split(',')) == 2:
                        airport_lng, airport_lat = open_airport[1].strip(',').split(',')
                    else:
                        print("错误的map_info", open_airport[1])
                        continue

                    distance = dist_from_coordinates(float(city_lng), float(city_lat), float(airport_lng), float(airport_lat))
                    logger.debug("城市ID:{0},机场ID:{1},城市与机场之间的距离{2}".format(result[0], open_airport[0], distance / 1000))
                    if distance / 1000 <= 100:
                        condition_1 = 1
                        condition_judge_1(open_airport[0], result[0], open_airport[2], cond_trans_degree_1,
                                          cond_inner_order_1, distance / 1000)

                    elif distance / 1000 <= 200:
                        condition_2 = 1
                        condition_judge_2(open_airport[0], result[0], open_airport[2], cond_trans_degree_2,
                                          cond_inner_order_2, distance / 1000)
                    elif distance / 1000 <= 300:
                        condition_3 = 1
                        condition_judge_3(open_airport[0], result[0], open_airport[2], cond_trans_degree_3,
                                          cond_inner_order_3, distance / 1000)
            print(cond_trans_degree_1, cond_trans_degree_2, cond_trans_degree_3)

            if cond_trans_degree_1:
                if len(cond_trans_degree_1) >= 2:
                    sort_key = min(cond_trans_degree_1.keys())
                    logger.debug(
                        "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                  cond_inner_order_1[sort_key][1]))
                    write_csv(result[0], cond_inner_order_1[sort_key][0])
                else:
                    if len(cond_inner_order_1) >= 2:
                        sort_key = min(cond_inner_order_1.keys())
                        logger.debug(
                            "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                      cond_inner_order_1[sort_key][1]))
                        write_csv(result[0], cond_inner_order_1[sort_key][0])
                    else:
                        degree_key = list(cond_trans_degree_1.keys())[0]
                        inner_key = list(cond_inner_order_1.keys())[0]
                        if cond_trans_degree_1[degree_key][1] < cond_inner_order_1[inner_key][1]:
                            write_csv(result[0], cond_trans_degree_1[degree_key][0])
                        else:
                            write_csv(result[0], cond_inner_order_1[inner_key][0])

            elif cond_trans_degree_2:
                if len(cond_trans_degree_2) >= 2:
                    sort_key = min(cond_trans_degree_2.keys())
                    logger.debug(
                        "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_2[sort_key][0],
                                                                  cond_inner_order_2[sort_key][1]))
                    write_csv(result[0], cond_trans_degree_2[sort_key][0])
                else:
                    if len(cond_inner_order_2) >= 2:
                        sort_key = min(cond_inner_order_2.keys())
                        logger.debug(
                            "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_2[sort_key][0],
                                                                      cond_inner_order_2[sort_key][1]))
                        write_csv(result[0], cond_inner_order_2[sort_key][0])
                    else:
                        degree_key = list(cond_trans_degree_2.keys())[0]
                        inner_key = list(cond_inner_order_2.keys())[0]
                        if cond_trans_degree_2[degree_key][1] < cond_inner_order_2[inner_key][1]:
                            write_csv(result[0], cond_trans_degree_2[degree_key][0])
                        else:
                            write_csv(result[0], cond_inner_order_2[inner_key][0])

            elif cond_trans_degree_3:
                if len(cond_trans_degree_3) >= 2:
                    sort_key = min(cond_trans_degree_3.keys())
                    logger.debug(
                        "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_3[sort_key][0],
                                                                  cond_inner_order_1[sort_key][1]))
                    write_csv(result[0], cond_trans_degree_3[sort_key][0])
                else:
                    if len(cond_inner_order_3) >= 2:
                        sort_key = min(cond_inner_order_3.keys())
                        logger.debug(
                            "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_3[sort_key][0],
                                                                      cond_inner_order_3[sort_key][1]))
                        write_csv(result[0], cond_inner_order_3[sort_key][0])
                    else:
                        degree_key = list(cond_trans_degree_3.keys())[0]
                        inner_key = list(cond_inner_order_3.keys())[0]
                        if cond_trans_degree_3[degree_key][1] < cond_inner_order_3[inner_key][1]:
                            write_csv(result[0], cond_trans_degree_3[degree_key][0])
                        else:
                            write_csv(result[0], cond_inner_order_3[inner_key][0])

            elif not condition_1 or not condition_2 or not condition_3:
                write_city_list(result)

        except Exception as e:
            print(e)
            city_conn.rollback()




if __name__ == '__main__':
    update_share_airport()
