#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
import pandas
from DBUtils.PooledDB import PooledDB
import sys
import glob
import csv
from collections import defaultdict
import json
from city.config import base_path, config, upload_path, test_config
base_data_config = {
    'host': '10.10.69.170',
    'user': 'mioji_admin',
    'passwd': 'miaoji1109',
    'db': 'base_data',
    'charset': 'utf8'
}
ota_location_config = {
    'host': '10.10.230.206',
    'user': 'mioji_admin',
    'passwd': 'mioji1109',
    'db': 'source_info',
    'charset': 'utf8'
}
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
    url = 'https://www.expedia.com/Hotel-Search?destination={0}&'
    try:
        destination = suggest['regionNames']['lastSearchName']
        url = url.format(destination)
        return url
    except Exception as e:
        return None
def get_booking_url(suggest):
    url = 'http://www.booking.com/searchresults.zh-cn.html?label=misc-aHhSC9cmXHUO1ZtqOcw05wS94870954985:pl:ta:p1:p2:ac:ap1t1:neg:fi:tikwd-11455299683:lp9061505:li:dec:dm&sid=2fabc4030e6b847b9ef3b059e24c6b83&aid=376390&error_url=http://www.booking.com/index.zh-cn.html?label=gen173nr-1FCAEoggJCAlhYSDNiBW5vcmVmcgV1c19kZYgBAZgBMsIBA2FibsgBDNgBAegBAfgBC6gCBA;sid=8ba5e9abe3eb9fcadf8e837d4d5a2464;sb_price_type=total&;&ss=Boucau&ssne=Boucau&ssne_untouched=Boucau&dest_id={0}&dest_type=city&checkin_year=&checkin_month=&checkin_monthday=&checkout_year=&checkout_month=&checkout_monthday=&no_rooms=&group_adults=&group_children=0&from_sf=1ss=Boucau&ssne=Boucau&ssne_untouched=Boucau&dest_id={0}&dest_type=city&checkin_year=&checkin_month=&checkin_monthday=&checkout_year=&checkout_month=&checkout_monthday=&no_rooms=&group_adults=&group_children=0&from_sf=1'
    try:
        dest_id = suggest['dest_id']
        url = url.format(dest_id)
        return url
    except Exception as e:
        return None
def get_daodao_url(suggest):
    try:
        url = suggest['url']
        url = ''.join(['https://www.tripadvisor.cn',url])
        return url
    except Exception as e:
        return None
def get_qyer_url(suggest):
    return suggest
def data_connection_pool(config):
    __conn = PooledDB(creator=pymysql,mincached=1,maxcached=20,**config)
    conn = __conn.connection()
    return conn

def from_ota_get_city(config,param):
    path = ''.join([upload_path, str(param), '/'])
    new_add_city_excel_file = glob.glob(path+'/*/新增城市.xlsx')[0]
    conn = data_connection_pool(config)
    cursor = conn.cursor()
    city_data = pandas.read_excel(new_add_city_excel_file,)
    city_names = city_data['name'].values
    city_names_en = city_data['name_en'].values
    # city_id_numbers = city_data['city_id_number'].values
    # city_ids = city_data['city_id'].values
    city_ids = city_data['id'].values
    city_infos = zip(city_ids,city_names,city_names_en)
    sources = ['ctrip','elong','agoda','booking','expedia','hotels','daodao','qyer']
    hotel_source = ['ctrip','elong','agoda','booking','expedia','hotels']
    poi_source = ['daodao','qyer']
    select_sql = "select s_city,source,suggest from Cityupline.ota_location where source=%s and s_city like %s"
    hotel_saves = []
    poi_saves = []
    for city_info in city_infos:
        hotel_save = {}
        poi_save = {}
        poi_save['id'] = hotel_save['id'] = str(city_info[0])
        poi_save['name'] = hotel_save['name'] = str(city_info[1])
        poi_save['name_en'] = hotel_save['name_en'] = str(city_info[2])

        for source in sources:
            cursor.execute(select_sql,(source,'%'+city_info[1]+'%'))
            result = cursor.fetchone()
            if not result:
                pass
            else:
                name,city_source,suggest = result
                if '{' in suggest or 'ctrip' in source:
                    if 'ctrip' in source:
                        get_url = getattr(sys.modules[__name__],'get_{0}_url'.format(source))
                        url = get_url(suggest)
                    elif 'qyer' in source:
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

        poi_saves.append(poi_save.copy())
        hotel_saves.append(hotel_save.copy())


    with open(path+'酒店配置.csv','w+') as hotel:
        writer = csv.DictWriter(hotel,fieldnames=['id','name','name_en','ctrip','elong','agoda','booking','expedia','hotels'])
        writer.writeheader()
        writer.writerows(hotel_saves)
    with open(path+'景点配置.csv','w+') as poi:
        writer = csv.DictWriter(poi,fieldnames=['id','name','name_en','daodao','qyer'])
        writer.writeheader()
        writer.writerows(poi_saves)
    return '酒店配置.csv','景点配置.csv'

def add_city_suggest(city_path):
    conn = data_connection_pool(test_config)
    cursor = conn.cursor()
    sel_sql = "select id from city where status_online <> 'Close'"
    cursor.execute(sel_sql, ())
    city_dicts = {int(id[0]): 1 for id in cursor.fetchall()}
    cursor.close()
    conn.close()

    city_data = pandas.read_excel(city_path,)
    city_id = city_data['id'].values
    city_names = city_data['name'].values
    city_countryIds = city_data['country_id'].values
    city_mapinfos = city_data['map_info'].values
    city_numbers = city_data['id'].values
    city_infos = list(zip(city_id, city_names,city_countryIds,city_mapinfos,city_numbers))
    real_city_infos = [city for city in city_infos if not city_dicts.get(city[0])]
    sources = ['ctrip', 'elong', 'agoda', 'booking', 'expedia', 'hotels', 'daodao', 'qyer']
    deletion_city_suggest = defaultdict(list)

    for source in sources:
        for city_info in real_city_infos:
            deletion_city_suggest[source].append(city_info)
    return deletion_city_suggest

if __name__ == "__main__":
    # temp_config = config
    # temp_config['db'] = 'add_city_681'

    add_city_suggest('/search/service/nginx/html/MioaPyApi/store/upload/702/城市更新新增/新增城市.xlsx')

