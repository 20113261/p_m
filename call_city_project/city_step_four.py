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
from my_logger import get_logger
from datetime import datetime
# from apscheduler.schedulers.background import BackgroundScheduler
# backgroudscheduler = BackgroundScheduler()

param = sys.argv[1]
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step4',path)

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report4=%s,step4=%s where id=%s"
    update_sql_after = "update city_order set step5=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def monitor_google_driver(collection_name,param, task_name):
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask'][collection_name]
    total_count = collection.find({}).count()
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    select_sql = "select step4 from city_order where id=%s"

    cursor.execute(select_sql)
    status_id = cursor.fetchone()[0]
    if int(status_id) == 2:
        results = collection.find({'$and':[{'finished':0},{'useds_times':{'$lt':7}},{'task_name':task_name}]})
        not_finish_num = results.count()

    if int(not_finish_num) / int(total_count) <= 0:
        update_step_report('', param, 1, 0)
        # job = backgroudscheduler.get_job('step4')
        # job.remove()


def task_start():
    logger.info('[step4][%s]======== start =======' % [param])
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        save_cityId = []
        data_name = ''.join(['add_city_',str(param)])
        config['db'] = data_name
        path = ''.join([base_path,param,'/','city_id.csv'])
        with open(path,'r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                save_cityId.append(row['city_id'])
        save_cityId = ['10001','10003','10005']
        logger.info('[step4][%s] 启动发任务' % [param])
        collection_name, task_name = google_driver(save_cityId,param,config)
        logger.info('[step4][%s] 任务已发完 %s %s' % [param, collection_name, task_name])

        with open('tasks.json', 'r+') as f:
            tasks = json.load(f)
            tasks[param] = [collection_name, task_name]
            f.seek(0)
            json.dump(tasks, f)
        logger.info('[step4][%s] tasks: %s' % [param, tasks])

        # job = backgroudscheduler.add_job(monitor_google_driver,trigger='cron',minute='*/2',hour='*',id='step4',kwargs={'collection_name':collection_name,'param':param, 'task_name': task_name})
        # backgroudscheduler.start()

        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        logger.info('[step4][%s]======== successed =======' % [param])
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        logger.info('[step4][%s]======== failed =======' % [param])
        update_step_report('', param, -1,0)

if __name__ == "__main__":
    task_start()

