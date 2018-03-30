#!/usr/bin/env python
# -*- coding:utf-8 -*-
import csv
import pymysql
import traceback
import numpy as np
from my_logger import get_logger
from service_platform_conn_pool import fetchall, base_data_pool,init_pool
from city.config import base_path
import traceback
import json
from collections import defaultdict
from my_logger import get_logger
import types
config = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


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


def get_need_share_airport_city(config):
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
    
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    try:
        cursor.execute(city_sql)
        citys = cursor.fetchall()
        cursor.execute(airport_sql)
        airports = cursor.fetchall()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()
    return citys, airports


def condition_judge_1(id, city_id, inner_value, trans_degree, inner_order, distance,config):
    city_conn = pymysql.connect(**config)
    cursor = city_conn.cursor()
    try:
        sql = "SELECT trans_degree FROM city WHERE id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("codition_judge_1数据查询出现错误", e)
        city_conn.rollback()
    finally:
        city_conn.close()
    temp_degree = result[0]
    if int(temp_degree) == -1:
        temp_degree = 100000

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


def condition_judge_2(id, city_id, inner_value, trans_degree, inner_order, distance,config):
    city_conn = pymysql.connect(**config)
    cursor = city_conn.cursor()
    try:
        sql = "SELECT trans_degree FROM city WHERE id=%s"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("condition_judge_2数据查询出现错误", e)
        city_conn.rollback()
    finally:
        city_conn.close()
    temp_degree = result[0]
    if int(temp_degree) == -1:
        temp_degree = 100000

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


def condition_judge_3(id, city_id, inner_value, trans_degree, inner_order, distance,config):
    city_conn = pymysql.connect(**config)
    cursor = city_conn.cursor()
    try:
        sql = "SELECT trans_degree FROM city WHERE id=%s;"
        cursor.execute(sql, (city_id,))
        result = cursor.fetchone()
    except Exception as e:
        print("codition_judge_3数据查询出现错误", e)
        city_conn.rollback()
    finally:
        city_conn.close()
    temp_degree = result[0]
    if int(temp_degree) == -1:
        temp_degree = 100000

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


def write_csv(city_id, _id,param,config,airport_info):
    path = ''.join([base_path, str(param), '/'])
    city_conn = pymysql.connect(**config)
    cursor = city_conn.cursor()
    city_sql = "SELECT id,country_id,status_online,map_info FROM city WHERE id = %s"
    airport_sql = "SELECT id,map_info,name,name_en,belong_city_id,iata_code FROM airport WHERE id =%s"
    try:
        cursor.execute(city_sql, (city_id,))
        city_result = cursor.fetchone()
        cursor.execute(airport_sql, (_id,))
        airport_result = cursor.fetchone()
        with open(path+'share_airport.csv', 'a+') as airport:
            writer = csv.writer(airport)
            writer.writerow((city_result[0], city_result[1], city_result[2], city_result[3], airport_result[0],
                             airport_result[1], airport_result[2], airport_result[3], airport_result[4]))
        if airport_info:
            airport_info[str(city_result[0])] = {'airport_iata_code':airport_result[5],'airport_name':airport_result[2],'airport_map_info':airport_result[1],
                                            'airport_name_en': airport_result[3],'airport_belong_city_id':airport_result[4],'airport_from':'生成共享机场'}
    except Exception as e:
        city_conn.rollback()
        print(e)
    finally:
        city_conn.close()


def write_city_list(city_id,param,config):
    city_conn = pymysql.connect(**config)
    path = ''.join([base_path, str(param), '/'])
    cursor = city_conn.cursor()
    select_sql = "SELECT id,name FROM city WHERE id=%s"
    try:
        cursor.execute(select_sql, (city_id,))
        results = cursor.fetchone()
    except Exception as e:
        city_conn.rollback()
    finally:
        city_conn.close()
    with open(path+'city_list.csv', 'a+') as city_list:
        writer = csv.writer(city_list)
        writer.writerow(results)


def update_share_airport(config,param,add_new_city=None,airport_info=None):

    path = ''.join([base_path, str(param), '/'])


    logger = get_logger('step3',path)
    with open(path+'city_list.csv', 'w+') as city:
        writer = csv.writer(city)
        writer.writerow(('city_id', 'city_name'))
    with open(path+'share_airport.csv', 'w+') as city:
        writer = csv.writer(city)
        writer.writerow(('city_id', 'country_id', 'status_online', 'city_mapInfo', 'airport_id', 'airport_mapInfo',
                         'name', 'name_en', 'belong_city_id'))
    city_conn = pymysql.connect(**config)
    cursor = city_conn.cursor()
    citys, airports = get_need_share_airport_city(config)
    if add_new_city:
        citys = add_new_city
    print('citys:',len(citys),'airports:',len(airports))
    select_city = 'SELECT map_info,country_id FROM city WHERE id = %s'

    for result in citys:
        if isinstance(result, (str,)):
            city_id = result
        else:
            city_id = result[0]
        cond_trans_degree_1 = {}
        cond_inner_order_1 = {}
        cond_trans_degree_2 = {}
        cond_inner_order_2 = {}
        cond_trans_degree_3 = {}
        cond_inner_order_3 = {}
        cursor.execute(select_city, result)
        city_mapInfo = cursor.fetchone()
        if len(city_mapInfo[0].strip(',').split(',')) == 2:
            city_lng, city_lat = city_mapInfo[0].strip(',').split(',')
        else:
            continue

        condition_1 = 0
        condition_2 = 0
        condition_3 = 0
        for open_airport in airports:
            if open_airport[3] == city_mapInfo[1]:
                if len(open_airport[1].strip(',').split(',')) == 2:
                    airport_lng, airport_lat = open_airport[1].strip(',').split(',')
                else:
                    print("错误的map_info", open_airport[1])
                    continue

                distance = dist_from_coordinates(float(city_lng), float(city_lat), float(airport_lng), float(airport_lat))

                if distance <= 100:
                    condition_1 = 1

                    condition_judge_1(open_airport[0], city_id, open_airport[2], cond_trans_degree_1,
                                      cond_inner_order_1, distance,config)

                elif distance <= 200:
                    condition_2 = 1

                    condition_judge_2(open_airport[0], city_id, open_airport[2], cond_trans_degree_2,
                                      cond_inner_order_2, distance,config)
                elif distance <= 300:
                    condition_3 = 1
                    condition_judge_3(open_airport[0], city_id, open_airport[2], cond_trans_degree_3,
                                      cond_inner_order_3, distance,config)

        if cond_trans_degree_1:
            if len(cond_trans_degree_1) >= 2:
                sort_key = min(cond_trans_degree_1.keys())
                write_csv(city_id, cond_inner_order_1[sort_key][0],param,config,airport_info)
            else:
                if len(cond_inner_order_1) >= 2:
                    sort_key = max(cond_inner_order_1.keys())
                    write_csv(city_id, cond_inner_order_1[sort_key][0],param,config,airport_info)
                else:
                    degree_key = list(cond_trans_degree_1.keys())[0]
                    inner_key = list(cond_inner_order_1.keys())[0]
                    if cond_trans_degree_1[degree_key][1] < cond_inner_order_1[inner_key][1]:
                        write_csv(city_id, cond_trans_degree_1[degree_key][0],param,config,airport_info)
                    else:
                        write_csv(city_id, cond_inner_order_1[inner_key][0],param,config,airport_info)

        elif cond_trans_degree_2:
            if len(cond_trans_degree_2) >= 2:
                sort_key = min(cond_trans_degree_2.keys())
                write_csv(city_id, cond_trans_degree_2[sort_key][0],param,config,airport_info)
            else:
                if len(cond_inner_order_2) >= 2:
                    sort_key = max(cond_inner_order_2.keys())
                    write_csv(city_id, cond_inner_order_2[sort_key][0],param,config,airport_info)
                else:
                    degree_key = list(cond_trans_degree_2.keys())[0]
                    inner_key = list(cond_inner_order_2.keys())[0]
                    if cond_trans_degree_2[degree_key][1] < cond_inner_order_2[inner_key][1]:
                        write_csv(city_id, cond_trans_degree_2[degree_key][0],param,config,airport_info)
                    else:
                        write_csv(city_id, cond_inner_order_2[inner_key][0],param,config,airport_info)

        elif cond_trans_degree_3:
            if len(cond_trans_degree_3) >= 2:
                sort_key = min(cond_trans_degree_3.keys())
                write_csv(city_id, cond_trans_degree_3[sort_key][0],param,config,airport_info)
            else:
                if len(cond_inner_order_3) >= 2:
                    sort_key = max(cond_inner_order_3.keys())
                    write_csv(city_id, cond_inner_order_3[sort_key][0],param,config,airport_info)
                else:
                    degree_key = list(cond_trans_degree_3.keys())[0]
                    inner_key = list(cond_inner_order_3.keys())[0]
                    if cond_trans_degree_3[degree_key][1] < cond_inner_order_3[inner_key][1]:
                        write_csv(city_id, cond_trans_degree_3[degree_key][0],param,config,airport_info)
                    else:
                        write_csv(city_id, cond_inner_order_3[inner_key][0],param,config,airport_info)

        elif not condition_1 or not condition_2 or not condition_3:
            write_city_list(city_id,param,config)

    return 'share_airport.csv','city_list.csv'

def get_airport_ids(config):
    config['db'] = 'add_city_' + config
    select_sql = "select id from airport where status = 'Open'"
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    cursor.execute(select_sql, ())
    result = {id: 1 for id in cursor.fetchall()}
    return result

def from_file_get_share_airport(config, param):
    # airport_ids = get_airport_ids(config)
    path = ''.join([base_path, str(param), '/'])
    airport_info = defaultdict(dict)
    city_id_map = {}
    city_id_map_copy = {}
    logger = get_logger('step3', path)
    with open(path+'city_id.csv','r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            city_id_map[str(row['city_id_number'])] = [row['city_id'],row['name']]
            city_id_map_copy[str(row['city_id_number'])] = [row['city_id'],row['name']]

    with open(path+'add_new_airport.csv','w+') as airport:
        writer = csv.writer(airport)
        writer.writerow(('iata_code','name','name_en','city_id','belong_city_id','map_info','status','time2city_center','inner_order'))

    with open(path+'add_new_share_airport.csv','w+') as airport:
        writer = csv.writer(airport)
        writer.writerow(('iata_code','name','name_en','city_id','belong_city_id','map_info','status','time2city_center','inner_order'))
    save_add_new_airport = []
    save_add_new_share_airport = []
    with open(path+'新增机场.csv','r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            # if airport_ids.get(row['id']):continue
            save_pop_key = str(row['city_id'])
            if not str(row['city_id']).isdigit() or not str(row['belong_city_id']).isdigit():
                if row['city_id'] and str(row['city_id']).isdigit():
                    row['city_id'] = city_id_map_copy[row['city_id']][0]
                if row['belong_city_id'] and str(row['belong_city_id']).isdigit():
                    row['belong_city_id'] = city_id_map_copy[row['belong_city_id']][0]
                save_add_new_airport.append((row['iata_code'], row['name'], row['name_en'],
                                             row['city_id'], row['belong_city_id'], row['map_info'], row['status'],
                                             row['time2city_center'], row['inner_order']))


            else:
                if row['city_id'] == row['belong_city_id']:
                    row['city_id'] = row['belong_city_id'] = city_id_map_copy[str(row['city_id'])][0]
                    if city_id_map.get(save_pop_key,None):
                        city_id_map.pop(save_pop_key)
                    save_add_new_airport.append((row['iata_code'], row['name'], row['name_en'],
                                                      row['city_id'], row['belong_city_id'], row['map_info'], row['status'],
                                                      row['time2city_center'], row['inner_order']))
                    airport_info[str(row['city_id'])] = {'airport_iata_code':row['iata_code'],'airport_map_info':row['map_info'],'airport_name':row['name'],
                                               'airport_name_en':row['name_en'],'airport_from':'标注机场','airport_belong_city_id':row['belong_city_id']
                                               }
                    # logger.debug("[city_id等于belong_city_id][{0}]".format(str(row)))
                elif row['city_id'] != row['belong_city_id']:

                    row['city_id'] = city_id_map_copy[str(row['city_id'])][0]
                    row['belong_city_id'] = city_id_map_copy[str(row['belong_city_id'])][0]

                    save_add_new_share_airport.append((row['iata_code'],row['name'],row['name_en'],row['city_id'],row['belong_city_id'],row['map_info'],row['status'],
                                                      row['time2city_center'],row['inner_order']))
                    if city_id_map.get(save_pop_key, None):
                        city_id_map.pop(save_pop_key)
                    airport_info[str(row['city_id'])] = {'airport_iata_code':row['iata_code'],'airport_map_info':row['map_info'],'airport_name':row['name'],
                                               'airport_name_en':row['name_en'],'airport_from':'标注共享机场','airport_belong_city_id':row['belong_city_id']
                                               }
                    # logger.debug("[city_id不等于belong_city_id][{0}]".format(str(row)))
        else:
            save_city_id = []
            if city_id_map:
                for key,value in city_id_map.items():
                    save_city_id.append(value[0])
                logger.debug("[需要共享机场的城市id][{0}]".format(str(city_id_map)))
    with open(path+'add_new_airport.csv', 'a+') as airport:
        writer = csv.writer(airport)
        for new_airport in save_add_new_airport:
            writer.writerow(new_airport)
    with open(path+'add_new_share_airport.csv', 'a+') as airport:
        writer = csv.writer(airport)
        for new_share_airport in save_add_new_share_airport:
            writer.writerow(new_share_airport)

    return 'add_new_airport.csv','add_new_share_airport.csv',save_city_id,airport_info
if __name__ == '__main__':
    from_file_get_share_airport('686')
