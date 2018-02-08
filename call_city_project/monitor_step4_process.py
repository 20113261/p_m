#coding:utf-8

from apscheduler.schedulers.blocking import BlockingScheduler
import pymongo
import pymysql
import json

from city.config import data_config, OpCity_config
from call_city_project.step_status import modify_status, getStepStatus

scheduler = BlockingScheduler()

def update_step_report(csv_path,param,step_front,step_after,step_num):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report"+str(step_num)+"=%s,step"+str(step_num)+"=%s where id=%s"
    update_sql_after = "update city_order set step"+str(step_num+1)+"=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def from_tag_get_tasks_status(tag):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    sql = "select * from service_platform_product_mongo_report where tag=%s"
    try:
        cursor.execute(sql, (tag,))
        result = cursor.fetchall()
    finally:
        conn.close()
    return result

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
            update_step_report('', param, -1, 0, 4)
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
                update_step_report('', param, 1, 0, 4)
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
            update_step_report('', param, -1, 0, 9)
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
                update_step_report('', param, 1, 0, 9)
                modify_status('step9', param, flag=False)

    print('running===============1')


def monitor_task5():
    tasks = getStepStatus('step5')
    print('-0-', tasks)
    for param, values in tasks.items():
        if len(values)==0:continue
        task_names = [val[1] for val in zip(*values)]
        print('-1-', task_names)
        tag = task_names.rsplit('_', 1)[-1]
        print('-2-', tag)
        tasks_status = from_tag_get_tasks_status(tag)
        print('-3-', tasks_status)
        if len(tasks_status) < len(task_names):
            print('-4-', '完蛋')
            continue
        status_list_len = []
        for _0, _1, _2, l_done, l_failed, _5, l_all, d_done, d_failed, _9, d_all, i_done, i_failed, i_all, _14 in tasks_status:
            if not (l_done+l_failed==l_all and d_done+d_failed==d_all and i_done+i_failed==i_all):
                print('-5-', '不行')
                break
            else:
                print('-6-', '可以', _1)
                status_list_len.append(1)
        if len(status_list_len)==len(task_names):
            update_step_report('', param, 1, 0, 5)
            print('=== %s === 任务完成' % tag)


def monitor_task8():
    pass


def local_jobs():
    scheduler.add_job(monitor_task4, 'cron', second='*/40', id='step4')
    scheduler.add_job(monitor_task9, 'cron', second='*/40', id='step9')

if __name__ == '__main__':
    # local_jobs()
    # scheduler.start()
    monitor_task5()