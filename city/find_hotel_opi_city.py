#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
import pandas
from DBUtils.PooledDB import PooledDB
import sys

import csv
from collections import defaultdict
import json


def get_elong_url(suggest):
    url = "http://ihotel.elong.com/region_{0}/"
    try:
        id = suggest['id']
        url = url.format(str(id))
        return url
    except Exception as e:
        return None

def get_ctrip_url(sugggest):
    url = "http://hotels.ctrip.com/international/{0}"
    try:
        city_info = sugggest.split('|')
        id = ''.join([city_info[4],city_info[5]])
        url = url.format(id)
        return url
    except Exception as e:
        return None

def get_agoda_url(suggest):
    url = "https://www.agoda.com"
    try:
        id = suggest['Url']
        url = '/'.join([url,id])
        return url
    except Exception as e:
        return None

def get_hotels_url(suggest):
    url = "https://www.hotels.cn/search.do?resolved-location=CITY:{0}:UNKNOWN:UNKNOWN&destination-id={1}&q-destination={2}"
    try:
        destinationId = suggest['destinationId']
        caption = suggest['caption']
        caption = caption.replace("<span class='highlighted'>",'').replace("</span>",'')
        url = url.format(destinationId,destinationId,caption)
        return url
    except Exception as e:
        return None
def get_expedia_url(suggest):
    url = 'https://www.expedia.com/Hotel-Search?destination={0}'
    try:
        destination = suggest['regionNames']['lastSearchName']
        url = url.format(destination)
        return url
    except Exception as e:
        return None
def get_booking_url():
    pass

def data_connection_pool():
    __conn = PooledDB(creator=pymysql,mincached=1,maxcached=20,host='10.10.228.253',user='mioji_admin',passwd='mioji1109',db='source_info',charset='utf8')
    conn = __conn.connection()
    return conn

def from_ota_get_city():
    conn = data_connection_pool()
    cursor = conn.cursor()
    city_data = pandas.read_csv('city_list.csv',)
    city_names = city_data['city_name'].values
    sources = ['ctrip','elong','agoda','booking','expedia','hotels','daodao','qyer']
    hotel_source = ['ctrip','elong','agoda','booking','expedia','hotels']
    poi_source = ['daodao','qyer']
    select_sql = "select s_city,source,suggest from ota_location where source=%s and s_city=%s"
    for city_name in city_names:
        hotel_save = {}
        poi_save = {}
        hotel_save['name'] = city_name
        poi_save['name'] = city_name
        for source in sources:
            cursor.execute(select_sql,(source,city_name))
            result = cursor.fetchone()
            if not result:
                pass
            else:
                name,city_source,suggest = result
                if '{' in suggest or 'ctrip' in source:
                    if 'ctrip' in source:
                        get_url = getattr(sys.modules[__name__],'get_{0}_url'.format(source))
                        url = get_url(suggest)
                    else:
                        suggest = json.loads(suggest)
                        get_url = getattr(sys.modules[__name__],'get_{0}_url'.format(source))
                        url = get_url(suggest)
                    if source in hotel_source:
                        hotel_save[source] = url
                    elif source in poi_source:
                        poi_save[source] = url

                elif source in hotel_source:
                    hotel_save[source] = suggest
                elif source in poi_source:
                    poi_save[source] = suggest
        with open('酒店配置.csv','a+') as hotel:
            writer = csv.DictWriter(hotel,fieldnames=['name','ctrip','elong','agoda','booking','expedia','hotels'])
            writer.writerow(hotel_save)
        with open('景点配置.csv','a+') as poi:
            writer = csv.DictWriter(poi,fieldnames=['name','daodao','qyer'])
            writer.writerow(poi_save)

def add_city_suggest():
    conn = data_connection_pool()
    cursor = conn.cursor()
    city_data = pandas.read_excel('新增城市.xlsx',)
    city_names = city_data['name'].values
    city_countryIds = city_data['country_id'].values
    city_mapinfos = city_data['map_info'].values
    city_names = list(zip(city_names,city_countryIds,city_mapinfos))
    sources = ['ctrip', 'elong', 'agoda', 'booking', 'expedia', 'hotels', 'daodao', 'qyer']
    deletion_city_suggest = defaultdict(list)
    select_sql = "select s_city,source,suggest from ota_location where source=%s and s_city=%s"

    for source in sources:
        for city_name in city_names:
            cursor.execute(select_sql, (source, str(city_name[0])))
            result = cursor.fetchone()
            if not result:
                deletion_city_suggest[source].append(city_name)
    return deletion_city_suggest

if __name__ == "__main__":
    with open('酒店配置.csv','w+') as hotel:
        writer = csv.writer(hotel)
        writer.writerow(('name','ctrip','elong','agoda','booking','expedia','hotels'))
    with open("景点配置.csv",'w+') as poi:
        writer = csv.writer(poi)
        writer.writerow(('name','daodao','qyer'))

    add_city_suggest()

