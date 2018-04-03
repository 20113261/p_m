#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
from MongoTask.MongoTaskInsert import InsertTask, TaskType
import requests
from lxml import html
import re
import sys
import json
config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'source_info',
    'charset': 'utf8'
}

google_config = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'db': 'base_data',
    'charset': 'utf8'
}
def create_task():
    conn = pymysql.connect(**config)
    daodao_sql = "select suggest from ota_location where source = 'daodao'"
    google_sql = "select distinct(hotel_name),hotel_name_en from base_data.hotel limit 1200"
    cursor = conn.cursor()
    cursor.execute(daodao_sql)
    daodao_values = cursor.fetchall()
    conn = pymysql.connect(**google_config)
    cursor = conn.cursor()
    cursor.execute(google_sql)
    google_values = cursor.fetchall()
    with InsertTask(worker='proj.total_tasks.other_source_hotel_url', queue='hotel_list', routine_key='hotel_list',
                    task_name='daodao_hotel_url_20180402a', source='daodao', _type='daodaoHotel',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for value in daodao_values[:1000]:
            url = value[0]
            url = url.replace('Tourism','Hotels').replace('Vacations','Hotels')
            args = {
                'url': url,
                'source': 'daodao',
                'spider_tag': 'daodaoListHotel',
                'data_from': 'daodao'

            }
            it.insert_task(args)
        # for value in google_values[:1000]:
        #     hotel_name = value[0]
        #     hotel_name_en = value[1]
        #     key_word = ','.join([hotel_name,hotel_name_en])
        #     args = {
        #         'url': key_word,
        #         'source': 'daodao',
        #         'spider_tag': 'googlelistspider',
        #         'data_from': 'google'
        #     }
        #     it.insert_task(args)

def  get_hotels_url(url):
    try:
        pattern = re.search(r'f-hotel-id=([0-9]+)',url)
        hotel_id = pattern.group(1)
        real_url = "https://ssl.hotels.cn/ho{0}/?pa=1&q-check-out=2018-04-16&tab=description&q-room-0-adults=2&YGF=7&q-check-in=2018-04-15&MGT=1&WOE=1&WOD=7&ZSX=0&SYE=3&q-room-0-children=0".format(hotel_id)
    except:
        return None,None
    return real_url

def get_agoda_url(url):
    try:
        content = requests.get(url).content.decode('utf-8')
        agoda_json = re.search(r'params.searchbox = (.*)(?=;)',content).group(1)
        agoda_json = json.loads(agoda_json)
        url = agoda_json.get('recentSearches',[])[0].get('data',{}).get('url')
        base_url = 'https://www.agoda.com'
        pattern = re.search(r'selectedproperty=([0-9]+)',url)
        hotel_id = pattern.group(1)
        real_url = ''.join([base_url,url])
    except:
        return None,None
    return real_url,hotel_id

def get_ctrip_url(url):
    return None,None

def get_booking_url(url):
    return None,None

def get_elong_url(url):
    content = requests.get(url).content.decode('utf-8')
    try:
        hotel_id = re.search(r'hotelId":"([0-9]+)"',content).group(1)
        real_url = 'http://ihotel.elong.com/{0}/'.format(hotel_id)
    except:
        return None,None
    return real_url,hotel_id

def get_direct_request_url(url):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Host': 'www.tripadvisor.cn',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
    }
    content = requests.get(url,headers=headers).content
    root = html.fromstring(content)
    real_url = root.xpath('//a[contains(@rel,"nofollow")]')[0].attrib.get('href')
    return real_url

def get_real_request_url(url,source):
    response = requests.get(url,timeout=240)
    real_url = response.url
    if source == 'booking':
        try:
            hotel_id = re.search(r'dmetadao-hotel-([0-9]+)',url).group(1)
        except:
            return None,None
        return real_url,hotel_id
    else:
        return real_url,None

def create_detail_task():
    config = {
        'host': '10.10.230.206',
        'user': 'mioji_admin',
        'password': 'mioji1109',
        'db': 'daodao_google',
        'charset': 'utf8'
    }
    source_map = {
        1: 'agoda',
        2: 'booking',
        3: 'ctrip',
        4: 'elong',
        5: 'hotels'
    }
    conn = pymysql.connect(**config)
    cursor = conn.cursor()
    select_sql = "select id,agoda,booking,ctrip,elong,hotels from daodao_hotel where other_source like '%,%' limit 1200"
    cursor.execute(select_sql)
    values = cursor.fetchall()
    cursor.close()
    conn.close()
    with InsertTask(worker='proj.total_tasks.daodao_detail_hotel', queue='poi_detail', routine_key='poi_detail',
                    task_name='daodao_detail_hotel', source='daodao', _type='daodaoHotel',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for value in values[:1000]:
            for i,source_url in enumerate(value[1:],1):
                if not source_url:
                    continue
                url = get_direct_request_url(source_url)
                real_url,source_id = get_real_request_url(url,source_map[i])
                if source_map[i] != 'booking':
                    get_url = getattr(sys.modules[__name__],'get_{0}_url'.format(source_map[i]))
                    real_url,source_id = get_url(url)
                if not real_url:
                    continue
                args = {
                    'url': real_url,
                    'source': source_map[i],
                    'data_from': 'daodao',
                    'city_id': 'NULL',
                    'source_id': source_id,
                    'hid': value[0],
                    'country_id': 'NULL'

                }
                it.insert_task(args)
if __name__ == "__main__":
    create_detail_task()