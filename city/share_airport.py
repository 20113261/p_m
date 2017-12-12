#!/usr/bin/env python
# -*- coding:utf-8 -*-


import csv
import pymysql

pymysql.install_as_MySQLdb()
import traceback
import pandas
import dataset
import copy
from logger import get_logger

config = {
    'host': '10.10.230.206',
    'port': 3306,
    'user': 'writer',
    'password': 'miaoji1109',
    'db': 'tmp',
    'charset': 'utf8'
}
import sys
import math
city_conn = pymysql.connect(**config)

EARTH_RADIUS = 6378137
PI = 3.1415927
logger = get_logger('city')
def rad(d):
    return d * PI / 180.0


def getDist(lng1, lat1, lng2, lat2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)

    s = 2 * math.asin(
        math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))

    s = s * EARTH_RADIUS
    s = round(s * 10000) / 10000

    return int(s)

def get_need_share_airport_city():
    select_sql = """SELECT id FROM city WHERE (status_test = 'Open' OR status_online = 'Open') AND id NOT IN (SELECT DISTINCT city_id
                                                                      FROM airport
                                                                      WHERE status = 'Open');"""
    cursor = city_conn.cursor()

    try:
        cursor.execute(select_sql)
        citys = cursor.fetchall()
    except Exception as e:
        print(e)
        city_conn.rollback()

    try:
        select_sql = """
                SELECT
              airport.id          AS id,
              airport.map_info    AS map_info,
              airport.inner_order AS inner_order,
              city.country_id     AS country_id
            FROM airport
              JOIN city ON airport.belong_city_id = city.id
            WHERE
              airport.status = 'Open' AND airport.name != '' AND airport.name_en != '' AND airport.city_id = airport.belong_city_id
        """
        cursor.execute(select_sql)
        airports = cursor.fetchall()
    except Exception as e:
        print(e)
        city_conn.rollback()

    return citys, airports


def condition_judge_1(id, city_id, inner_value, trans_degree, inner_order, distance):
    cursor = city_conn.cursor()
    try:
        sql = "select trans_degree from city where id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("codition_judge_1数据查询出现错误", e)
        city_conn.rollback()
    temp_degree = result[0]
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
        sql = "select trans_degree from city where id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("condition_judge_2数据查询出现错误", e)
        city_conn.rollback()

    temp_degree = result[0]
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
        sql = "select trans_degree from city where id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("codition_judge_3数据查询出现错误", e)
        city_conn.rollback()
    temp_degree = result[0]
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


def write_toCsv(city_id, id):
    cursor = city_conn.cursor()
    city_sql = "select id,country_id,status_online,map_info from city where id = %s"
    airport_sql = "select id,map_info,name,name_en,belong_city_id from airport where id =%s"
    try:
        cursor.execute(city_sql, (city_id,))
        city_result = cursor.fetchone()
        cursor.execute(airport_sql, (id,))
        airport_result = cursor.fetchone()
        with open('share_airprot.csv', 'a+') as airport:
            writer = csv.writer(airport)
            writer.writerow((city_result[0], city_result[1], city_result[3], city_result[2], airport_result[0],
                             airport_result[2], airport_result[3], airport_result[1], airport_result[4]))
    except Exception as e:
        city_conn.rollback()
        print(e)


def write_city_list(result):
    cursor = city_conn.cursor()
    select_sql = "select id,name from city where id=%s"
    try:
        cursor.execute(select_sql,result)
        results = cursor.fetchone()
    except Exception as e:
        city_conn.rollback()
    with open('city_list.csv', 'a+') as city_list:
        writer = csv.writer(city_list)
        writer.writerow(results)


def update_share_airport():
    with open('city_list.csv','w+') as city:
        writer = csv.writer(city)
        writer.writerow(('city_id','city_name'))
    with open('share_aiprot.csv','w+') as city:
        writer = csv.writer(city)
        writer.writerow(('city_id','country_id','status_online','city_mapInfo','airport_id','airport_mapInfo','name','name_en','belong_city_id'))
    cursor = city_conn.cursor()
    citys, airports = get_need_share_airport_city()

    select_city = 'select map_info,country_id from city where id = %s'

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

                    distance = getDist(float(city_lng), float(city_lat), float(airport_lng), float(airport_lat))
                    logger.debug("城市ID:{0},机场ID:{1},城市与机场之间的距离{2}".format(result[0], open_airport[0], distance))
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

            print(cond_trans_degree_1,cond_trans_degree_2,cond_trans_degree_3)
            if cond_trans_degree_1:
                if len(cond_trans_degree_1) >= 2:
                    sort_key = max(cond_trans_degree_1.keys())
                    logger.debug(
                        "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                  cond_inner_order_1[sort_key][1]))
                    write_toCsv(result[0], cond_inner_order_1[sort_key][0])
                else:
                    if len(cond_inner_order_1) >= 2:
                        sort_key = max(cond_inner_order_1.keys())
                        logger.debug(
                            "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                      cond_inner_order_1[sort_key][1]))
                        write_toCsv(result[0], cond_inner_order_1[sort_key][0])
                    else:
                        sort_key = max(cond_trans_degree_1.keys())
                        write_toCsv(result[0], cond_trans_degree_1[sort_key][0])

            elif cond_trans_degree_2:
                if len(cond_trans_degree_2) >= 2:
                    sort_key = max(cond_trans_degree_2.keys())
                    logger.debug(
                        "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                  cond_inner_order_1[sort_key][1]))
                    write_toCsv(result[0], cond_trans_degree_2[sort_key][0])
                else:
                    if len(cond_inner_order_2) >= 2:
                        sort_key = max(cond_inner_order_2.keys())
                        logger.debug(
                            "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                      cond_inner_order_1[sort_key][1]))
                        write_toCsv(result[0], cond_inner_order_2[sort_key][0])
                    else:
                        sort_key = max(cond_trans_degree_2.keys())
                        write_toCsv(result[0], cond_trans_degree_2[sort_key][0])

            elif cond_trans_degree_3:
                if len(cond_trans_degree_3) >= 2:
                    sort_key = max(cond_trans_degree_3.keys())
                    logger.debug(
                        "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                  cond_inner_order_1[sort_key][1]))
                    write_toCsv(result[0], cond_trans_degree_3[sort_key][0])
                else:
                    if len(cond_inner_order_3) >= 2:
                        sort_key = max(cond_inner_order_3.keys())
                        logger.debug(
                            "城市ID：{0},机场ID：{1},城市与机场之间的距离：{2}".format(result[0], cond_inner_order_1[sort_key][0],
                                                                      cond_inner_order_1[sort_key][1]))
                        write_toCsv(result[0], cond_inner_order_3[sort_key][0])
                    else:
                        sort_key = max(cond_trans_degree_3.keys())
                        write_toCsv(result[0], cond_trans_degree_3[sort_key][0])

            elif not condition_1 or not condition_2 or not condition_3:
                write_city_list(result)

        except Exception as e:
            print(e)
            city_conn.rollback()


def insert_airport(path=None):
    cursor = city_conn.cursor()
    select_sql = "select * from airport where iata_code=%s"
    update_sql = "update airport set name=%s,name_en=%s,city_id=%s,belong_city_id=%s,map_info=%s,status=%s,time2city_center=%s,inner_order=%s where iata_code=%s"
    insert_sql = "insert into airport(iata_code,name,name_en,city_id,belong_city_id,map_info,status,time2city_center,inner_order) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    with open(path, 'r+') as airport:
        reader = csv.reader(airport)
        next(reader)
        for row in reader:
            try:
                count = cursor.execute(select_sql, (row[0],))
                print('count:', count, 'iata_code:', row[0])
                map_info = row[5].replace('，', ',').split(',')
                map_info = ','.join([map_info[1].strip(), map_info[0].strip()])
                finally_result = cursor.fetchall()
                if count == 0:
                    print('insert:', row[0])
                    cursor.execute(insert_sql,
                                   (row[0], row[1], row[2], row[3], row[4], map_info, row[6], row[7], row[8]))
                    city_conn.commit()
                elif count == 1:
                    print('update:', row[0])
                    cursor.execute(update_sql,
                                   (row[1], row[2], row[3], row[4], map_info, row[6], row[7], row[8], row[0]))
                    city_conn.commit()
                else:
                    print('many:', row[0])
                    with open('many_airport.csv', 'a+') as many:
                        wirter = csv.writer(many)
                        for res in finally_result:
                            wirter.writerow(res)
            except Exception as e:
                print(traceback.format_exc())
                city_conn.rollback()


def city():
    cursor = city_conn.cursor()
    select_sql = "select id,name,name_en,map_info from city where id =%s"
    with open('city_list.csv', 'r+') as city:
        reader = csv.reader(city)
        for row in list(reader):
            try:
                cursor.execute(select_sql, row)
                results = cursor.fetchall()
                with open('share_city.csv', 'a+') as share_city:
                    writer = csv.writer(share_city)
                    for result in results:
                        writer.writerow(result)
            except:
                print(row)
                city_conn.rollback()


if __name__ == '__main__':
    pass