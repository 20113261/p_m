#coding:utf-8

from apscheduler.schedulers.blocking import BlockingScheduler
import pymongo
import pymysql
import json

from city.config import base_path,config,OpCity_config
from call_city_project.step_status import modify_status, getStepStatus

scheduler = BlockingScheduler()

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

def monitor_task4():
    print('running===============0')
    tasks = getStepStatus('step4')

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

        select_sql = "select step4 from city_order where id=%s"
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
                modify_status('step4', param, flag=False)


    print('running===============1')

def monitor_task9():
    print('running===============0')
    tasks = getStepStatus('step9')

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
                modify_status('step9', param, flag=False)

    print('running===============1')


def monitor_task5():
    pass

def monitor_task8():
    pass


def local_jobs():
    scheduler.add_job(monitor_task4, 'cron', second='*/40', id='step4')
    scheduler.add_job(monitor_task9, 'cron', second='*/40', id='step9')

if __name__ == '__main__':
    local_jobs()
    scheduler.start()