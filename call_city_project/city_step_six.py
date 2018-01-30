#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
from city.config import OpCity_config,base_path,config
from collections import defaultdict
import json
import traceback
import sys
import csv
import os
from city.generate_urls import inner_city
from city.inter_city_task import city_inter_google_driver
import multiprocessing
import pymongo
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
backgroudscheduler = BackgroundScheduler()
def update_step_report(csv_path,param,step):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql = "update city_order set report5=%s,step5=%s where id=%s"
    try:
       cursor.execute(update_sql,(csv_path,step,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

@backgroudscheduler.scheduled_job('cron',id='step6',minute='*/2',hour='*')
def monitor_google_driver(collection_name):
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask'][collection_name]
    total_count = collection.find({})
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    select_sql = "select step4 from city_order where id=%s"
    while True:
        cursor.execute(select_sql)
        status_id = cursor.fetchone()[0]
        if int(status_id) == 2:

            results = collection.find({'$or': [{'finished': 0},{'$and':{'finished':0,'used_times':{'$lt':7}}}]})
            not_finish_num = results.count()
            if not not_finish_num:
                break
    if not_finish_num / total_count <= 0:
        return True
    else:
        return False

def task_start():
    try:
        param = sys.argv[1]
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        data_name = ''.join(['add_city_',str(param)])
        config['db'] = data_name
        save_cityId = []
        path = ''.join([base_path, param, '/', 'city_id.csv'])
        with open(path, 'r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                save_cityId.append(row['city_id'])

        #获取google url
        save_cityId = ['10001','10003','10007']
        inner_city(cid_list=save_cityId,config=config)

        file_list = os.listdir('./urls/')
        if '.DS_Store' in file_list:
            file_list.remove('.DS_Store')
        save_urls = []
        for city_id in save_cityId:
            for child_path in file_list:
                if city_id in child_path:
                    with open('./urls/{0}'.format(child_path),'r+') as city:
                        urls = city.readlines()
                        save_urls.extend(urls)

        collection_name = city_inter_google_driver(save_urls,param)
        judge = monitor_google_driver(collection_name)
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report('', param, 1)
        return judge
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report('', param, -1)


if __name__ == "__main__":
    task_start()