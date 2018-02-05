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
def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report6=%s,step6=%s where id=%s"
    update_sql_after = "update city_order set step7=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def monitor_google_driver(collection_name,param):
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask'][collection_name]
    total_count = collection.find({})
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    select_sql = "select step6 from city_order where id=%s"

    cursor.execute(select_sql)
    status_id = cursor.fetchone()[0]
    if int(status_id) == 2:

        results = collection.find({'finished': 0})
        not_finish_num = results.count()

    if not_finish_num / total_count <= 0:
        job = backgroudscheduler.get_job('step6')
        job.remove()
        update_step_report('',param,1,0)

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

        collection_name,task_name = city_inter_google_driver(save_urls,param)
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'r+') as f:
            tasks = json.load(f)
            tasks[param] = [collection_name, task_name]
            f.seek(0)
            json.dump(tasks, f)

        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))

    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report('', param, -1,0)


if __name__ == "__main__":
    task_start()