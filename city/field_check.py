#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
import csv
import math
from functools import reduce
from city.add_city import read_file
from collections import defaultdict
from toolbox import Common
from datetime import datetime
import pandas
import re
import pypinyin
city_path = "测试新增城市.csv"
airport_path = "测试新增机场.csv"


config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'base_data',
    'charset': 'utf8'
}
check_field = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'db': 'base_data',
    'charset': 'utf8'
}

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

def city_must_write_field(city_path):
    check_not_empty_field = [
        'name','name_en','tri_code','py','map_info','time_zone','summer_zone','summer_start',
         'summer_end','grade','summer_start_next_year','summer_end_next_year','country_id',
         'trans_degree','new_product_city_pic'
         ]
    new_add_city = pandas.read_csv(city_path,encoding='utf-8',)
    citys = new_add_city['name'].values
    city_field_empty = defaultdict(list)
    for field in check_not_empty_field:
        bool_list = new_add_city[field].isnull()
        empty_field_city = new_add_city[bool_list]
        empty_city = empty_field_city['name'].values
        for city in empty_city:
            city_field_empty[city].append(field)
    print(city_field_empty)
    if city_field_empty:
        with open('check_empty_city.csv','w+') as city:
            writer = csv.writer(city)
            writer.writerow(('国家名','空字段'))
            for key,value in city_field_empty.items():
                value_str = ','.join(value)
                writer.writerow((key,value_str))
        return False
    return True
def city_field_check(city_path):
    trans_degree = [-1, 0, 1]
    status = ['Close', 'Open']
    not_standard_field = defaultdict(list)
    select_tricode = "select * from city where tri_code=%s"
    select_country = "select name from country where mid=%s"
    select_province = "select * from province where id=%s"
    select_region = "select * from region where id=%s"
    judge_empty_field = city_must_write_field(city_path)
    if not judge_empty_field:
        return ''
    with open(city_path) as city:
        reader = csv.DictReader(city)
        for row in reader:
            conn = pymysql.connect(**check_field)
            cursor = conn.cursor()

            #检查中,英文名
            judge_name = Common.has_any(row['name'],Common.is_chinese)
            if not judge_name:
                not_standard_field['name'].append(row['name'])
            judge_name_en = Common.is_all(row['name_en'],Common.is_latin_and_punctuation)
            if not judge_name_en:
                not_standard_field['name_en'].append(row['name_en'])

            #检查城市三字码
            try:
                cursor.execute(select_tricode,(row['tri_code']))
                judge_tricode = cursor.fetchall()
            except Exception as e:
                raise e
            if judge_tricode:
                not_standard_field['tri_code'].append(row['name'])

            #检查别名
            alias = row.get('alias',None)
            if alias and alias != 'NULL':
                try:
                    judge_alias = re.search(r'^[a-zA-Z|]+$',alias).group()
                except:
                    not_standard_field['alias'].append(row['name'])
            #检查拼音
            pinyin = pypinyin.lazy_pinyin(row['name'])
            pinyin_str = ''.join(pinyin)
            if pinyin_str != row['py']:
                not_standard_field['py'].append(row['name'])

            #检查 time_zone,summer_zone
            time_zone = int(row['time_zone'])
            summer_zone = int(row['summer_zone'])
            if time_zone < -11 and time_zone > 14:
                not_standard_field['time_zone'].append(row['name'])
            if summer_zone < -11 and time_zone > 14:
                not_standard_field['summer_zone'].append(row['name'])
            if time_zone > summer_zone:
                not_standard_field['time_summer'].append(row['name'])

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
                    not_standard_field[key].append(row['name'])

            #检查国家以及省份
            try:
                cursor.execute(select_country,(row['country_id'],))
                country_name = cursor.fetchone()
                if not country_name:
                    not_standard_field['country_id'].append(row['name'])
                if country_name[0] in ['美国','日本','加拿大']:
                    province = row.get('prov_id',None)
                    if not province:
                        not_standard_field['prov_id'].append(row['name'])
                    elif province != 'NULL':
                        cursor.execute(select_province,(province,))
                        judge_province = cursor.fetchall()
                        if not judge_province:
                            not_standard_field['prov_id'].append(row['name'])
                else:
                    province = row.get('prov_id',None)
                    if province and province != 'NULL':
                        cursor.execute(select_province, (province,))
                        judge_province = cursor.fetchall()
                        if not judge_province:
                            not_standard_field['prov_id'].append(row['name'])
            except Exception as e:
                conn.rollback()

            #检查 grade
            grade = int(row['grade'])
            if grade < 1 and grade > 9:
                not_standard_field['grade'].append(row['name'])

            #检查trans_degree
            tran_degree = int(row['trans_degree'])
            if tran_degree not in trans_degree:
                not_standard_field['trans_degree'].append(row['name'])

            #检查各种环境状态
            save_status = {}
            save_status['status_online'] = row.get('status_online',None)
            save_status['status_test'] = row.get('status_test',None)
            save_status['dept_status_online'] = row.get('dept_status_online',None)
            save_status['dept_status_test'] = row.get('dept_status_test',None)
            for key,value in save_status.items():
                if value and value not in status:
                    save_status[key].append(row['name'])

            #检查region_id
            region_id = row.get('region_id',None)
            if region_id and region_id != 'NULL':
                cursor.execute(select_region,(region_id,))
                judge_region = cursor.fetchall()
                if not judge_region:
                    not_standard_field['region_id'].append(row['name'])

            # 检查 其他字段
            others_field = {}
            others_field['is_schegen'] = row.get('is_schegen',None)
            others_field['is_park'] = row.get('is_park',None)
            others_field['official'] = row.get('official',None)
            for key,value in others_field.items():
                if value and int(value) not in [0,1]:
                    not_standard_field[key].append(row['name'])

            #检查 new_product_city_pic
            save_value = []
            new_product_city_pic= row['new_product_city_pic']
            if 'default.jpg' in new_product_city_pic:
                not_standard_field['new_product_city_pic'].append(row['name'])
            else:
                pic_list = new_product_city_pic.split('|')
                for pic in pic_list:
                    try:
                        pic_pattern = re.search(r'([0-9]+)_([0-9]+)',pic)
                        save_value.append(int(pic_pattern.group(2)))
                    except:
                        not_standard_field['new_product_city_pic'].append(row['name'])
                        break
                total_value = reduce(lambda x,y:x+y,range(min(save_value),max(save_value)+1))
                total = reduce(lambda x,y:x+y,save_value)
                if total != total_value:
                    not_standard_field['new_product_city_pic'].append(row['name'])
            conn.close()
    with open('check_city.csv','a+') as city:
        writer = csv.writer(city)
        writer.writerow(('字段名称','不合格国家'))
        for key,value in not_standard_field.items():
            value_str = ','.join(value)
            writer.writerow((key,value_str))

def airport_must_write_field(airport_path):
    check_not_empty_field = [
        'iata_code','name','name_en','belong_city_id','map_info','inner_order'
    ]
    new_add_airport = pandas.read_csv(airport_path, encoding='utf-8', )
    airport_field_empty = defaultdict(list)
    for field in check_not_empty_field:
        bool_list = new_add_airport[field].isnull()
        empty_field_airport = new_add_airport[bool_list]
        empty_airport = empty_field_airport['name'].values
        for airport in empty_airport:
            airport_field_empty[airport].append(field)
    print(airport_field_empty)
    if airport_field_empty:
        with open('check_empty_airport.csv', 'w+') as airport:
            writer = csv.writer(airport)
            writer.writerow(('机场名', '空字段'))
            for key, value in airport_field_empty.items():
                value_str = ','.join(value)
                writer.writerow((key, value_str))
        return False
    return True


def airport_field_check(airport_path):
    select_tricode = "select * from airport where iata_code=%s"
    select_city = "select map_info from city where id=%s"
    not_standard_field = defaultdict(list)
    status_list = ['Open','Close']
    with open(airport_path,'r+') as airport:
        reader = csv.DictReader(airport)
        for row in reader:
            conn = pymysql.connect(**config)
            cursor = conn.cursor()

            # 检查中,英文名
            judge_name = Common.has_any(row['name'], Common.is_chinese)
            if not judge_name:
                not_standard_field['name'].append(row['name'])
            judge_name_en = Common.is_all(row['name_en'], Common.is_latin_and_punctuation)
            if not judge_name_en:
                not_standard_field['name_en'].append(row['name_en'])

            #检查机场三字码
            try:
                cursor.execute(select_tricode,(row['iata_code'],))
                judge_airport = cursor.fetchall()
            except Exception as e:
                raise e
            if not judge_airport:
                not_standard_field['iata_code'].append(row['name'])

            #检查belong_city_id以及map_info
            belong_cityId = row['belong_city_id']
            try:
                cursor.execute(select_city,(belong_cityId,))
                map_info = cursor.fetchone()
            except Exception as e:
                conn.rollback()
                raise e
            if not map_info:
                not_standard_field['belong_city_id'].append(row['name'])
            elif map_info[0] != 'NULL':
                city_long,city_lat = map_info[0].split(',')
                airport_long,airport_lat = row['map_info'].split(',')
                distince = getDist(float(city_long),float(city_lat),float(airport_long),float(airport_lat))
                if distince > 200000:
                    not_standard_field['map_info'].append(row['name'])


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
                    not_standard_field['city_id'].append(row['name'])

            #检查 status
            status = row.get('status',None)
            if status and status not in status_list:
                not_standard_field['status'].append(row['name'])

            #检查 time2city_center
            time2city_center = row.get('time2city_center',None)
            if time2city_center and not str(time2city_center).isdigit():
                not_standard_field['time2city_center'].append(row['name'])

            #检查 inner_order
            inner_order = int(row['inner_order'])
            if inner_order >= 1 or inner_order == -1:
                pass
            else:
                not_standard_field['inner_order'].append(row['name'])

    with open('check_airport.csv','w+') as airport:
        writer = csv.writer(airport)
        writer.writerow(('字段名称','机场名称'))
        for key,value in not_standard_field.items():
            value_str = ','.join(value)
            writer.writerow((key,value_str))


def check_repeat_city():
    pass

def check_repeat_airport():
    pass
if __name__ == "__main__":
    airport_field_check(airport_path)