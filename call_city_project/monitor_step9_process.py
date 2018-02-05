#!/usr/bin/env python
# -*- coding:utf-8 -*-

from city.config import OpCity_config
import pymysql
pymysql.install_as_MySQLdb()
import json
import pymongo


from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler()
def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report9=%s,step9=%s where id=%s"
    update_sql_after = "update city_order set step10=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def monitor_task():
    print('running===============0')
    with open('tasks.json') as f:
        tasks = json.load(f)

    tasks_list = list(tasks.items())
    if len(tasks_list) == 0:
        return

    collection_name = tasks_list[0][1][0]
    print('0==========', collection_name)
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask'][collection_name]
    total_count = collection.find({}).count()
    print('01==========', total_count)
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    for param, (_, task_name) in tasks.items():
        print(param, (_, task_name))
        if total_count == 0:
            update_step_report('', param, -1, 0)
            return

        select_sql = "select step9 from city_order where id=%s"
        print('1==========', select_sql, param)
        cursor.execute(select_sql, (param))
        status_id = cursor.fetchone()[0]
        print('2==========', status_id)

        if int(status_id) == 2:
            results = collection.find(
                {'$and':[
                    {'finished':1},
                    {'useds_times':{'$lt':7}},
                    {'task_name':task_name}
                ]
                },
                hint=[('task_name', 1), ('finished', 1), ('used_times', 1)])
            not_finish_num = results.count()
            print('3==========', not_finish_num)

            if int(not_finish_num) / int(total_count) <= 0:
                update_step_report('', param, 1, 0)
                job = scheduler.get_job('step4')
                job.remove()

    print('running===============1')

def add_job():
    scheduler.add_job(monitor_task, 'cron', second='*/40', id='step4')

if __name__ == "__main__":
    add_job()
    scheduler.start()