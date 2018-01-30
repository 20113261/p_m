#!/usr/local/bin/python3
# -*- coding:utf-8 -*-
import pymysql
pymysql.install_as_MySQLdb()
from city.config import base_path,config,OpCity_config
import os
import sys
import csv
from city.inter_city_task import google_driver
from collections import defaultdict
import json
import traceback
import pymongo
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
backgroudscheduler = BackgroundScheduler()
def update_step_report(csv_path,param,step):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql = "update city_order set report4=%s,step4=%s where id=%s"
    try:
       cursor.execute(update_sql,(csv_path,step,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

@backgroudscheduler.scheduled_job('cron', minute='*/2',hour='*',id='step4')
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
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        param = sys.argv[1]
        save_cityId = []
        data_name = ''.join(['add_city_',str(param)])
        config['db'] = data_name
        path = ''.join([base_path,param,'/','city_id.csv'])
        with open(path,'r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                save_cityId.append(row['city_id'])

        collection_name = google_driver(save_cityId,param,config)
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