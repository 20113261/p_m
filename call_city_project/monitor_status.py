#coding:utf-8

from apscheduler.schedulers.blocking import BlockingScheduler
import pymongo
import pymysql
import json
import datetime
import os

from city.config import data_config, OpCity_config, base_path
from call_city_project.step_status import modify_status, getStepStatus
from my_logger import get_logger

scheduler = BlockingScheduler()
logger = get_logger('monitor', base_path)


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

def monitor_task_summary(step):
    stepa = 'step'+step
    logger.info('================= ' + stepa + ' ================= 开始')
    tasks = getStepStatus(stepa)
    for param, values in tasks.items():
        if len(values) == 0: continue
        task_name = values[-1]
        logger.info('{}, {}'.format(stepa, task_name))
        tasks_status = from_tag_get_tasks_status(task_name)
        logger.info('{}, {}'.format(stepa, tasks_status))
        line = tasks_status[0]
        t_all, t_done, t_failed = line[3], line[4], line[5]
        if t_all == t_done + t_failed:
            update_step_report('', param, 1, 0, int(step))
            modify_status(stepa, param, flag=False)
            logger.info('================= ' + stepa + ' ================= 完成')
    logger.info('================= ' + stepa + ' ================= 1')

def monitor_report(step):
    stepa = 'step' + step
    logger.info('================= ' + stepa + ' ================= 开始')
    tasks = getStepStatus(stepa)
    for param, values in tasks.items():
        if len(values)==0:continue
        task_names = zip(*values)[0]
        logger.info('{}, {}'.format(stepa, task_names))
        tag = str(task_names[0].rsplit('_', 1)[-1])
        logger.info('{}, {}'.format(stepa, tag))
        tasks_status = from_tag_get_tasks_status(tag, True)
        finaled_date = max(a[-1] for a in tasks_status)
        all_finaled_data = [a for a in tasks_status if a[-1]==finaled_date]
        logger.info(stepa, tasks_status)
        if len(tasks_status) < len(task_names):
            logger.info('{}, {}'.format(stepa, '任务状态数 小于 任务已发任务数'))
            continue
        status_list_len = 0
        for (_0, source, _2, l_done, l_failed, _5, l_all, d_done, d_failed, d_all, i_done, i_failed, i_all, _13) in all_finaled_data:
            #规则 1 完成任务数 + 失败任务数 = 任务总数
            #规则 2 失败任务数 == 任务总数 发邮件报警
            if not (l_done+l_failed==l_all and d_done+d_failed==d_all and i_done+i_failed==i_all) or l_failed==l_all or d_failed==d_all:
                if source in ('Qyer', 'Daodao'):continue
                logger.info('{}, {}: {}'.format(stepa, '未完成', source))
                break
            else:
                logger.info('{}, {}: {}'.format(stepa, '已完成', source))
                status_list_len+=1
        if status_list_len==len(task_names)-2:
            update_step_report('', param, 1, 0, int(step))
            modify_status(stepa, param, flag=False)
            logger.info('================= ' + stepa + ' ================= 完成')
    logger.info('================= ' + stepa + ' ================= 1')


def local_jobs():
    # scheduler.add_job(monitor_task9, 'date', next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=2), id='test')
    scheduler.add_job(monitor_task_summary, 'cron', args=('4',), second='*/300', next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=83), id='step4')
    scheduler.add_job(monitor_task_summary, 'cron', args=('9',), second='*/300', next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=23), id='step9')
    scheduler.add_job(monitor_report, 'cron', args=('5',), second='*/300', id='step5')
    scheduler.add_job(monitor_task_summary, 'cron', args=('8',), second='*/300', id='step8')


if __name__ == '__main__':
    local_jobs()
    scheduler.start()
    # monitor_task5()
    # monitor_task8()