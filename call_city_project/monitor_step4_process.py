#coding:utf-8

from apscheduler.schedulers.blocking import BlockingScheduler
import pymongo
import pymysql
import json
import datetime

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

def from_tag_get_tasks_status(name, flag=False):
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    sql_step5 = "select * from service_platform_product_mongo_report where tag=%s"
    sql_step8 = "select * from serviceplatform_product_mongo_split_task_summary where task_name=%s"
    sql = sql_step5 if flag else sql_step8
    try:
        cursor.execute(sql, (name,))
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
        task_names = zip(*values)[0]
        print('-1-', task_names)
        tag = str(task_names[0].rsplit('_', 1)[-1])
        print('-2-', tag)
        tasks_status = from_tag_get_tasks_status(tag, True)
        finaled_date = max(a[-1] for a in tasks_status)
        all_finaled_data = [a for a in tasks_status if a[-1]==finaled_date]
        print('-3-', tasks_status)
        if len(tasks_status) < len(task_names):
            print('-4-', '完蛋')
            continue
        status_list_len = 0
        for (_0, _1, _2, l_done, l_failed, _5, l_all, d_done, d_failed, d_all, i_done, i_failed, i_all, _13) in all_finaled_data:
            #规则 1 完成任务数 + 失败任务数 = 任务总数
            #规则 2 失败任务数 == 任务总数 发邮件报警
            if not (l_done+l_failed==l_all and d_done+d_failed==d_all and i_done+i_failed==i_all) or l_failed==l_all or d_failed==d_all:
                if _1 in ('Qyer', 'Daodao'):continue
                print('-5-', '不行')
                break
            else:
                print('-6-', '可以', _1)
                status_list_len+=1
        if status_list_len==len(task_names)-2:
            update_step_report('', param, 1, 0, 5)
            modify_status('step5', param, flag=False)
            print('=== %s === 任务完成' % tag)


def monitor_task8():
    tasks = getStepStatus('step8')
    print('-0-', tasks)
    for param, values in tasks.items():
        if len(values)==0:continue
        task_name = values[-1]
        print('-1-', task_name)
        tasks_status = from_tag_get_tasks_status(task_name)
        print('-2-', tasks_status)
        line = tasks_status[0]
        print('-3-', line)
        t_all, t_done, t_failed = line[3], line[4], line[5]
        if t_all==t_done+t_failed:
            update_step_report('', param, 1, 0, 8)
            modify_status('step8', param, flag=False)
            print('-4-', '完成')



def local_jobs():
    # scheduler.add_job(monitor_task5, 'date', next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=2), id='test')
    scheduler.add_job(monitor_task4, 'cron', second='*/40', id='step4')
    scheduler.add_job(monitor_task9, 'cron', second='*/40', id='step9')
    scheduler.add_job(monitor_task5, 'cron', second='*/80', id='step5')
    scheduler.add_job(monitor_task8, 'cron', second='*/80', id='step8')


if __name__ == '__main__':
    local_jobs()
    scheduler.start()
    # monitor_task5()
    # monitor_task8()