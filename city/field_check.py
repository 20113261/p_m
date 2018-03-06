#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
import csv
import math
import traceback
import copy
from functools import reduce
from collections import defaultdict
from toolbox import Common
from datetime import datetime
import dataset
import pandas
import re
import pypinyin
import json
from city.config import base_path
from my_logger import get_logger
from service_platform_conn_pool import source_info_pool, fetchall
from datetime import datetime
import hashlib
import os
MAX_DISTANCE = 20000

check_field = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'db': 'base_data',
    'charset': 'utf8'
}
logger = get_logger('city')
def rad(d):
    PI = 3.1415927
    return d * PI / 180.0

def getDist(lng1, lat1, lng2, lat2):
    EARTH_RADIUS = 6378137
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)

    s = 2 * math.asin(
        math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))

    s = s * EARTH_RADIUS
    s = round(s * 10000) / 10000

    return int(s)

#检查城市必填字段
def city_must_write_field(city_path,param):
    path = ''.join([base_path, str(param), '/'])

    check_not_empty_field = [
        'name','name_en','py','map_info','time_zone','summer_zone','summer_start',
         'summer_end','grade','summer_start_next_year','summer_end_next_year','country_id',
         'trans_degree',
         ]
    city_data = pandas.read_excel(city_path,)
    city_data.to_csv(path+'新增城市.csv', encoding='utf-8', index=False)
    new_add_city = pandas.read_csv(path+'新增城市.csv',encoding='utf-8',)
    citys = new_add_city['name'].values
    city_field_empty = defaultdict(list)
    for field in check_not_empty_field:
        bool_list = new_add_city[field].isnull()
        empty_field_city = new_add_city[bool_list]
        empty_city = empty_field_city['name'].values
        for city in empty_city:
            city_field_empty[city].append(field)
    with open(path+'check_empty_city.csv','w+') as city:
        writer = csv.writer(city)
        writer.writerow(('国家名','空字段'))
        for key,value in city_field_empty.items():
            value_str = ','.join(value)
            writer.writerow((key,value_str))
    if city_field_empty:
        return '/'.join([param,'check_empty_city.csv'])
    else:
        return None

#城市字段检查
def city_field_check(city_path,param,picture_path):
    path = ''.join([base_path, str(param), '/'])

    trans_degree = [-1, 0, 1]
    status = ['Close', 'Open']
    not_standard_field = defaultdict(list)
    select_tricode = "select * from city where tri_code=%s"
    select_country = "select name from country where mid=%s"
    select_province = "select * from province where id=%s"
    select_region = "select * from region where id=%s"
    city_data = pandas.read_excel(city_path,)
    city_data.to_csv(path+'新增城市.csv',encoding='utf-8',index=False)

    with open(path+'新增城市.csv') as city:
        reader = csv.DictReader(city)
        for row in reader:
            conn = pymysql.connect(**check_field)
            cursor = conn.cursor()

            #检查中,英文名
            judge_name = Common.has_any(row['name'],Common.is_chinese)
            if not judge_name:
                not_standard_field['name'].append((row['id'],row['name']))
            judge_name_en = Common.is_all(row['name_en'],Common.is_latin_and_punctuation)
            if not judge_name_en:
                not_standard_field['name_en'].append((row['id'],row['name']))

            #检查城市三字码
            try:
                cursor.execute(select_tricode,(row['tri_code']))
                judge_tricode = cursor.fetchall()
            except Exception as e:
                raise e
            if judge_tricode:
                not_standard_field['tri_code'].append((row['id'],row['name']))

            #检查别名
            alias = row.get('alias',None)
            if alias and alias != 'NULL':
                try:
                    judge_alias = re.search(r'^[a-zA-Z|]+$',alias).group()
                except:
                    not_standard_field['alias'].append((row['id'],row['name']))
            #检查拼音
            pinyin = pypinyin.lazy_pinyin(row['name'])
            pinyin_str = ''.join(pinyin)
            if pinyin_str != row['py']:
                not_standard_field['py'].append((row['id'],row['name']))

            #检查 time_zone,summer_zone
            time_zone = int(row['time_zone'])
            summer_zone = int(row['summer_zone'])
            if time_zone < -11 and time_zone > 14:
                not_standard_field['time_zone'].append((row['id'],row['name']))
            if summer_zone < -11 and time_zone > 14:
                not_standard_field['summer_zone'].append((row['id'],row['name']))
            if time_zone > summer_zone:
                not_standard_field['time_summer'].append((row['id'],row['name']))

            #检查时间格式
            save_time = {}
            save_time['summer_start'] = row['summer_start']
            save_time['summer_end'] = row['summer_end']
            save_time['summer_start_next_year'] = row['summer_start_next_year']
            save_time['summer_end_next_year'] = row['summer_end_next_year']
            for key,value in save_time.items():
                try:
                    datetime.strptime(value,'%Y-%m-%dT%H:%M:%S')
                except:
                    not_standard_field[key].append((row['id'],row['name']))

            #检查国家以及省份
            try:
                cursor.execute(select_country,(row['country_id'],))
                country_name = cursor.fetchone()
                if not country_name:
                    not_standard_field['country_id'].append((row['id'],row['name']))
                if country_name[0] in ['美国','日本','加拿大']:
                    province = row.get('prov_id',None)
                    if not province:
                        not_standard_field['prov_id'].append((row['id'],row['name']))
                    elif province != 'NULL':
                        cursor.execute(select_province,(province,))
                        judge_province = cursor.fetchall()
                        if not judge_province:
                            not_standard_field['prov_id'].append((row['id'],row['name']))
                else:
                    province = row.get('prov_id',None)
                    if province and province != 'NULL':
                        cursor.execute(select_province, (province,))
                        judge_province = cursor.fetchall()
                        if not judge_province:
                            not_standard_field['prov_id'].append((row['id'],row['name']))
            except Exception as e:
                conn.rollback()

            #检查 grade
            grade = int(row['grade'])
            if grade < 1 and grade > 9:
                not_standard_field['grade'].append((row['id'],row['name']))

            #检查trans_degree
            tran_degree = int(row['trans_degree'])
            if tran_degree not in trans_degree:
                not_standard_field['trans_degree'].append((row['id'],row['name']))

            #检查各种环境状态
            save_status = {}
            save_status['status_online'] = row.get('status_online',None)
            save_status['status_test'] = row.get('status_test',None)
            save_status['dept_status_online'] = row.get('dept_status_online',None)
            save_status['dept_status_test'] = row.get('dept_status_test',None)
            for key,value in save_status.items():
                if value and value not in status:
                    save_status[key].append((row['id'],row['name']))

            #检查region_id
            region_id = row.get('region_id',None)
            if region_id and region_id != 'NULL':
                cursor.execute(select_region,(region_id,))
                judge_region = cursor.fetchall()
                if not judge_region:
                    not_standard_field['region_id'].append((row['id'],row['name']))

            # 检查 其他字段
            others_field = {}
            others_field['is_schegen'] = row.get('is_schegen',None)
            others_field['is_park'] = row.get('is_park',None)
            others_field['official'] = row.get('official',None)
            for key,value in others_field.items():
                if value and int(value) not in [0,1]:
                    not_standard_field[key].append((row['id'],row['name']))


            # save_value = []
            # new_product_city_pic= row['new_product_city_pic']
            # if 'default.jpg' in new_product_city_pic:
            #     not_standard_field['new_product_city_pic'].append((row['id'],row['name']))
            # else:
            #     pic_list = new_product_city_pic.split('|')
            #     for pic in pic_list:
            #         try:
            #             pic_pattern = re.search(r'([0-9]+)_([0-9]+)',pic)
            #             save_value.append(int(pic_pattern.group(2)))
            #         except:
            #             not_standard_field['new_product_city_pic'].append((row['id'],row['name']))
            #             break
            #     total_value = reduce(lambda x,y:x+y,range(min(save_value),max(save_value)+1))
            #     total = reduce(lambda x,y:x+y,save_value)
            #     if total != total_value:
            #         not_standard_field['new_product_city_pic'].append((row['id'],row['name']))
            conn.close()
    # 检查图片名
    file_list = os.listdir(picture_path)
    if '.DS_Store' in file_list:
        file_list.remove('.DS_Store')
    for child in file_list:
        try:
            pic_pattern = re.search(r'([0-9]+)_([0-9]+)', child)
            city_id = pic_pattern.group(1)
            city_id_number = pic_pattern.group(2)
        except:
            not_standard_field['product'].append((child,))

    with open(path+'check_city.csv','w+') as city:
        writer = csv.writer(city)
        writer.writerow(('字段名称','不合格城市'))
        for key,value in not_standard_field.items():
            value_str = json.dumps(value)
            writer.writerow((key,value_str))
    if not_standard_field:
        return '/'.join([param,'check_city.csv'])
    else:
        return False

#检查机场必填字段
def airport_must_write_field(airport_path,param):
    path = ''.join([base_path, str(param), '/'])
    check_not_empty_field = [
        'iata_code','name','name_en','belong_city_id','map_info','inner_order'
    ]
    city_data = pandas.read_excel(airport_path, )
    city_data.to_csv(path+'新增机场.csv', encoding='utf-8', index=False)
    new_add_airport = pandas.read_csv(path+'新增机场.csv', encoding='utf-8', )
    airport_field_empty = defaultdict(list)
    for field in check_not_empty_field:
        bool_list = new_add_airport[field].isnull()
        empty_field_airport = new_add_airport[bool_list]
        empty_airport = empty_field_airport['name'].values
        for airport in empty_airport:
            airport_field_empty[airport].append(field)

    with open(path+'check_empty_airport.csv', 'w+') as airport:
        writer = csv.writer(airport)
        writer.writerow(('机场名', '空字段'))
        for key, value in airport_field_empty.items():
            value_str = ','.join(value)
            writer.writerow((key, value_str))
    if airport_field_empty:
        return '/'.join([param,'check_empty_airport.csv'])
    else:
        return False
#机场字段检查
def airport_field_check(airport_path,param):
    path = ''.join([base_path, str(param), '/'])
    select_tricode = "select * from airport where iata_code=%s"
    select_city = "select map_info from city where id=%s"
    not_standard_field = defaultdict(list)
    status_list = ['Open','Close']
    city_data = pandas.read_excel(airport_path, )
    city_data.to_csv(path+'新增机场.csv', encoding='utf-8', index=False)
    with open(path+'新增机场.csv','r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            conn = pymysql.connect(**check_field)
            cursor = conn.cursor()

            # 检查中,英文名
            judge_name = Common.has_any(row['name'], Common.is_chinese)
            if not judge_name:
                not_standard_field['name'].append((row['id'],row['name']))
            judge_name_en = Common.is_all(row['name_en'], Common.is_latin_and_punctuation)
            if not judge_name_en:
                not_standard_field['name_en'].append((row['id'],row['name']))

            #检查机场三字码
            try:
                cursor.execute(select_tricode,(row['iata_code'],))
                judge_airport = cursor.fetchall()
            except Exception as e:
                raise e
            if judge_airport:
                not_standard_field['iata_code'].append((row['id'],row['name']))

            #检查belong_city_id以及map_info
            belong_cityId = row['belong_city_id']
            try:
                cursor.execute(select_city,(belong_cityId,))
                map_info = cursor.fetchone()
            except Exception as e:
                conn.rollback()
                raise e
            if not map_info:
                not_standard_field['belong_city_id'].append((row['id'],row['name']))
            elif map_info[0] != 'NULL':
                city_long,city_lat = map_info[0].split(',')
                airport_long,airport_lat = row['map_info'].replace('，',',').split(',')
                distince = getDist(float(city_long),float(city_lat),float(airport_long),float(airport_lat))
                if distince > 200000:
                    not_standard_field['map_info'].append((row['id'],row['name']))


            #检查city_id
            city_id = row['city_id']
            if city_id and city_id != 'NULL':
                judge_city = None
                try:
                    cursor.execute(select_city,(city_id,))
                    judge_city = cursor.fetchall()
                except Exception as e:
                    conn.rollback()
                if not judge_city:
                    not_standard_field['city_id'].append((row['id'],row['name']))

            #检查 status
            status = row.get('status',None)
            if status and status not in status_list:
                not_standard_field['status'].append((row['id'],row['name']))

            #检查 time2city_center
            time2city_center = row.get('time2city_center',None)
            if time2city_center and not str(time2city_center).isdigit():
                not_standard_field['time2city_center'].append((row['id'],row['name']))

            #检查 inner_order
            inner_order = int(row['inner_order'])
            if inner_order >= 1 or inner_order == -1:
                pass
            else:
                not_standard_field['inner_order'].append((row['id'],row['name']))
    with open(path+'check_airport.csv','w+') as airport:
        writer = csv.writer(airport)
        writer.writerow(('字段名称','不合格机场'))
        for key,value in not_standard_field.items():
            value_str = json.dumps(value)
            writer.writerow((key,value_str))
    if not_standard_field:
        return '/'.join([param,'check_airport.csv'])
    else:
        return False

#检查城市是否重复
def check_repeat_city(city_path,param):

    path = ''.join([base_path, str(param), '/'])
    select_sql = "select * from city where name=%s and country_id=%s"
    judge_prov_id = "select * from city where name=%s and country_id =%s and prov_id=%s"
    judge_region_id = "select * from city where name=%s and country_id =%s and region_id=%s"
    select_mapInfo = "select map_info from city"
    conn = pymysql.connect(**check_field)
    cursor = conn.cursor()
    repeat_city = defaultdict(list)
    try:
        cursor.execute(select_mapInfo)
        mapInfo_list = cursor.fetchall()
    except Exception as e:
        conn.rollback()
    mapInfo_list = [map_info[0] for map_info in mapInfo_list]
    city_data = pandas.read_excel(city_path, )
    city_data.to_csv(path+'新增城市.csv', encoding='utf-8', index=False,)
    with open(path+'新增城市.csv','r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            long,lat = row['map_info'].replace('，',',').split(',')
            name = row['name']
            country_id = row['country_id']
            region_id = row['region_id']
            prov_id = row['prov_id']
            if not region_id and not prov_id:
                cursor.execute(select_sql,(name,country_id))
            elif region_id:
                cursor.execute(judge_region_id,(name,country_id,region_id))
            elif prov_id:
                cursor.execute((judge_prov_id,(name,country_id,prov_id)))
            judge_city = cursor.fetchall()
            if judge_city:
                for map_info in mapInfo_list:
                    if not map_info:
                        continue
                    city_long,city_lat = map_info.replace('，',',').split(',')
                    distance = getDist(float(long),float(lat),float(city_long),float(city_lat))
                    if distance <= MAX_DISTANCE:
                        repeat_city['repeat_city'].append((row['id'],row['name']))
                        break
    with open(path+'check_repeat_city.csv','w+') as city:
        writer = csv.writer(city)
        writer.writerow(('repeat_city','重复城市列表'))
        for key,value in repeat_city.items():
            value_str = json.dumps(value)
            writer.writerow((key,value_str))
    if repeat_city:
        return '/'.join([param,'check_repeat_city.csv'])
    else:
        return False

#检查是否机场重复
def check_repeat_airport(airport_path,param):
    path = ''.join([base_path, str(param), '/'])
    select_sql = "select * from airport where name=%s"
    select_mapInfo = "select map_info from airport"
    conn = pymysql.connect(**check_field)
    cursor = conn.cursor()
    repeat_airport = defaultdict(list)
    try:
        cursor.execute(select_mapInfo)
        mapInfo_list = cursor.fetchall()
    except Exception as e:
        conn.rollback()
    mapInfo_list = [map_info[0] for map_info in mapInfo_list]
    city_data = pandas.read_excel(airport_path, )
    city_data.to_csv(path+'新增机场.csv', encoding='utf-8', index=False)
    with open(path+'新增机场.csv','r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            long,lat = row['map_info'].replace('，',',').split(',')
            name = row['name']
            cursor.execute(select_sql,(name,))
            judge_airport = cursor.fetchall()
            if judge_airport:
                for map_info in mapInfo_list:
                    
                    if not map_info or map_info=='NULL':
                        continue
                    airport_long,airport_lat = map_info.replace('，',',').split(',')
                    distance = getDist(float(long),float(lat),float(airport_long),float(airport_lat))
                    if distance <= MAX_DISTANCE:
                        repeat_airport['repeat_airport'].append((row['id'],row['name']))
                        break

        with open(path+'check_repeat_airport.csv','w+') as airport:
            writer = csv.writer(airport)
            writer.writerow(('repeat_airport','重复机场列表'))
            for key,value in repeat_airport.items():
                value_str = json.dumps(value)
                writer.writerow((key,value_str))
        if repeat_airport:
            return '/'.join([param,'check_repeat_airport.csv'])
        else:
            return False
#城市字段不合格率统计
def not_standard_city_field_count():
    city_data = pandas.read_csv('新增城市.csv',encoding='utf-8')
    max_num = city_data.index.values.max() + 1
    count_result = defaultdict(dict)
    with open('check_city.csv','r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            city_list = json.loads(row['不合格城市'])
            city_num = len(city_list)
            field_name = row['字段名称']
            count_result[field_name]['not_city_pass'] = (city_num / max_num) * 100
            count_result[field_name]['city_pass'] = 100 - count_result[field_name]['not_city_pass']
            count_result[field_name]['source'] = 'city'
            count_result[field_name]['content'] = row['不合格城市']
    return count_result


#机场字段不合格率统计
def not_standard_airport_field_count():
    airport_data = pandas.read_csv('新增机场.csv', encoding='utf-8')
    max_num = airport_data.index.values.max() + 1
    count_result = defaultdict(dict)
    with open('check_airport.csv', 'r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            airport_list = json.loads(row['不合格机场'])
            airport_num = len(airport_list)
            field_name = row['字段名称']
            count_result[field_name]['not_airport_pass'] = airport_num / max_num
            count_result[field_name]['airport_pass'] = 1 - count_result[field_name]['not_airport_pass']
            count_result[field_name]['source'] = 'airport'
            count_result[field_name]['content'] = row['不合格机场']
    return count_result

def new_airport_insert(config,param):
    path = ''.join([base_path, str(param), '/'])
    db = dataset.connect('mysql+pymysql://{user}:{password}@{host}:3306/{db}?charset=utf8'.format(**config))
    airport_table = db['airport']
    table = pandas.read_csv(path+'新增机场.csv')

    _count = 0
    for i in range(len(table)):
        line = table.iloc[i]

        line = line.astype('str',inplace=True)
        line = line.to_dict()
        del line['utime']
        # airport_table.insert(line)
        airport_table.upsert(line, keys=['id'])

    db.commit()
    return True

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

def get_sid(source,suggest,city_id):
    if source == 'daodao':
        suggest = suggest
        _l_sid = re.findall('-(g\d+)', suggest)
        if _l_sid:
            sid = _l_sid[0]
        else:
            logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))
    elif source == 'qyer':
        suggest = suggest
        _l_sid = re.findall('http://place.qyer.com/(\S+?)/', suggest)
        if _l_sid:
            sid = _l_sid[0]
        else:
            logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))

    elif source == 'agoda':
        suggest = suggest
        _l_sid = re.findall('city=(\d+)', suggest)
        if _l_sid:
            sid = _l_sid[0]
        else:
            _l_sid = re.findall('cid=(\d+)', suggest)
            if _l_sid:
                sid = _l_sid[0]
    elif source == 'elong':
        suggest = suggest
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

    elif source == 'ctrip':
        suggest = suggest
        _l_sid = re.findall('http://hotels.ctrip.com/international/([\s\S]+)', suggest)
        if _l_sid:
            sid = _l_sid[0]
        else:
            logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))

    elif source == 'booking':
        suggest = suggest
        _l_sid = re.findall('dest_id=([-\d]+)&', suggest)
        if _l_sid:
            sid = _l_sid[0]
        else:
            logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))

    elif source == 'expedia':
        suggest = suggest
        _l_sid = re.findall('regionId=([\d]+)', suggest)
        if _l_sid:
            sid = _l_sid[0]
        else:
            sid = 'city-{}'.format(city_id)
            logger.info("[unknown suggest][source: {}][suggest: {}]".format(source, suggest))

    elif source == 'hotels':
        suggest = suggest
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
                
    return sid



def add_others_source_city(city_path,hotels_path,attr_path,config,param):
    select_country = "select name from country where mid=%s"
    conn = pymysql.connect(**check_field)
    cursor = conn.cursor()
    path = ''.join([base_path, str(param), '/'])
    city_info = {}
    source_city_info = defaultdict(dict)
    sources = ['agoda','ctrip','elong','hotels','expedia','booking']
    save_result = []
    with open(path+'city_id.csv','r+') as map:
        reader = csv.DictReader(map)
        for row in reader:

            city_info[row['city_id_number']] = row['city_id']
    with open(hotels_path,'r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            for source in sources:
                source_city_info[source][row['id']] = row[source]
    sources = ['qyer','daodao']
    with open(attr_path,'r+') as attr:
        reader = csv.DictReader(attr)
        for row in reader:
            print(row)
            for source in sources:
                source_city_info[source][row['id']] = row[source]

    insert_sql = "insert ignore into ota_location(source,sid_md5,sid,suggest_type,suggest,city_id,country_id,s_city,s_region,s_country,s_extra,label_batch,others_info) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    with open(path+'新增城市.csv','r+') as city:
        reader = csv.DictReader(city)
        for row in reader:
            for source in source_city_info.keys():
                others_info = {}
                city_id = city_info[row['id']]
                country_id = row['country_id']
                cursor.execute(select_country,(country_id,))
                country_name = cursor.fetchone()[0]
                city_name = row['name']
                suggest = source_city_info[source][row['id']]
                if not suggest:
                    continue
                label_batch = str(datetime.now())[:10].replace('-','')
                label_batch = ''.join([label_batch,'a'])
                sid = get_sid(source,suggest,city_id)
                md5 = hashlib.md5()
                md5.update(sid.encode('utf-8'))
                sid_md5 = md5.hexdigest()
                map_info = row['map_info']
                others_info['map_info'] = map_info
                others_info = json.dumps(others_info)
                save_result.append(
                    (source,sid_md5,sid,1,suggest,city_id,country_id,city_name,'NULL',country_name,'NULL',label_batch,others_info)
                )
    conn.close()
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    print(save_result)
    cursor.executemany(insert_sql,save_result)
    conn.commit()
    conn.close()
if __name__ == "__main__":
    from city.config import config
    config['db'] = ''.join(['add_city_','12345'])
