#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
import json
from city.config import OpCity_config
import pymongo
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler()
def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report3=%s,step3=%s where id=%s"
    update_sql_after = "update city_order set step4=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def get_total_count(collection_name):
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask'][collection_name]
    total_count = collection.find({}).count()
    return total_count

def monitor_step3_bark(stepa):
    tasks = getStepStatus(stepa)
    if len(tasks_list) == 0:
        return
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    save_result = []
    for param, collection_names in tasks.items():

        select_sql = "select step3 from city_order where id=%s"

        cursor.execute(select_sql, (param))
        status_id = cursor.fetchone()
        for collection_name,task_name in collection_names:
            total_count = get_total_count(collection_name)
            if int(total_count) == 0:
                update_step_report('',param,-1,0)
                return

        client = pymongo.MongoClient(host='10.10.231.105')
        collection = client['MongoTask'][collection_name]
        for collection_name, task_name in collection_names:
            if int(status_id) == 2:
                results = collection.find(
                    {'$and': [
                        {'finished': 1},
                        {'useds_times': {'$lt': 7}},
                        {'task_name': task_name}
                    ]
                    },
                    hint=[('task_name', 1), ('finished', 1), ('used_times', 1)])
                not_finish_num = results.count()

                if int(not_finish_num) / int(total_count) <= 0:
                    save_result.append(int(not_finish_num) / int(total_count))
    if max(save_result) <= 0:
        update_step_report('', param, 1, 0)
        job = scheduler.get_job('step3')
        job.remove()


if __name__ == "__main__":
    pass