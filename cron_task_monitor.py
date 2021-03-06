#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/13 下午10:46
# @Author  : Hou Rong
# @Site    : 
# @File    : cron_task_monitor.py
# @Software: PyCharm
import time
import traceback
import logging
import requests
import functools
import inspect
import asyncio
import pymongo
from logging import getLogger, StreamHandler, FileHandler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from service_platform_report.task_progress_report_mongo import task_progress_report_main as task_progress_report_mongo
from service_platform_report.task_progress_report import main as task_progress_report
from service_platform_report.crawl_data_check_script import detectOriData
from service_platform_report.data_coverage import data_coverage
from serviceplatform_data.create_view import create_all_view
from serviceplatform_data.insert_final_data import insert_data as detail_insert_data
from serviceplatform_data.image_insert_final_data import insert_data as image_insert_data
from serviceplatform_data.load_final_data import main as load_final_data
from serviceplatform_data.load_final_data_test import main as load_final_data_qyer
from service_platform_report.routine_report import main as routine_report
from service_platform_report.send_error_email import send_error_report_email
from serviceplatform_data.insert_data_mongo import insert_hotel_data, insert_city_data
from serviceplatform_data.get_nearby_hotel_city import get_nearby_city
from serviceplatform_data.update_hotel_validation import UpdateHotelValidation
from serviceplatform_data.insert_poi_detect_task_info import get_task_info
from serviceplatform_data.delete_already_scanned_file import delete_already_scanned_file
from my_logger import get_logger
from service_platform_report.merge_report import poi_merged_report
from service_platform_report.task_progress_report_mongo_split_task import task_progress_report_split_task_main

SEND_TO = ['zhangxiaopeng@mioji.com', "luwanning@mioji.com"]

logger = get_logger('cron_task_monitor')


def send_email(title, mail_info, mail_list, want_send_html=False):
    try:
        mail_list = ';'.join(mail_list)
        data = {
            'mailto': mail_list,
            'content': mail_info,
            'title': title,
        }
        if want_send_html:
            data['mail_type'] = 'html'
        requests.post('http://10.10.150.16:9000/sendmail', data=data)
    except Exception as e:
        logger.exception(msg="[send email error]", exc_info=e)
        return False
    return True


def on_exc_send_email(func):
    @functools.wraps(func)
    def wrapper():
        func_name = func.__name__
        try:
            func_file = inspect.getabsfile(func)
        except Exception as exc:
            logger.exception(msg="[get file exc]", exc_info=exc)
            try:
                func_file = inspect.getfile(func)
            except Exception as exc:
                logger.exception(msg="[get local file exc]", exc_info=exc)
                func_file = 'may be local func: {}'.format(func_name)
        try:
            logger.debug('[异常监控]统计及数据入库例行 执行 [file: {}][func: {}]'.format(func_file, func_name))
            func()
            logger.debug('[异常监控]统计及数据入库例行 执行完成 [file: {}][func: {}]'.format(func_file, func_name))
        except Exception as exc:
            logger.exception(msg="[run func or send email exc]", exc_info=exc)
            send_email('[异常监控]统计及数据入库例行 异常',
                       '[file: {}][func: {}] \n\n\n {}'.format(func_file, func_name, traceback.format_exc()), SEND_TO)

    return wrapper


def detail_insert_final_data():
    create_all_view()
    detail_insert_data(limit=5000)


def image_insert_final_data():
    image_insert_data(limit=5000)


def get_near_city():
    insert_hotel_data()
    insert_city_data()
    get_nearby_city()


def test_exc():
    raise Exception()


def poi_merge_report_total():
    poi_merged_report('attr')
    poi_merged_report('shop')


def start_update_hotel_validation():
    update_hotel_validation = UpdateHotelValidation()
    update_hotel_validation.start()


'''
#1 * * * * /usr/local/bin/python3 /search/hourong/PycharmProjects/PoiCommonScript/service_platform_report/routine_report.py >> /root/data/PycharmProjects/PoiCommonScript/service_platform_report/task_routine_log
'''

schedule = BackgroundScheduler()
schedule.add_job(on_exc_send_email(task_progress_report_mongo), 'cron', hour='*', minute='*/10',
                 id='task_progress_report_mongo',
                 max_instances=10)
schedule.add_job(on_exc_send_email(task_progress_report), 'cron', hour='*/2', id='task_progress_report',
                 max_instances=10)
schedule.add_job(on_exc_send_email(detectOriData), 'cron', hour='*', minute='*/30', id='detectOriData',
                 max_instances=10)
schedule.add_job(on_exc_send_email(data_coverage), 'cron', hour='*', minute='*/30', id='data_coverage')
schedule.add_job(on_exc_send_email(detail_insert_final_data), 'cron', minute='*/2', id='detail_insert_final_data')
schedule.add_job(on_exc_send_email(image_insert_final_data), 'cron', second='*/50', id='image_insert_final_data')
# schedule.add_job(on_exc_send_email(load_final_data), 'cron', minute='*/1', id='load_final_data')
schedule.add_job(on_exc_send_email(routine_report), 'cron', hour='*/1', id='routine_report', max_instances=10)
# schedule.add_job(on_exc_send_email(load_final_data_qyer), 'cron', second='*/20', id='routine_report_qyer')
schedule.add_job(on_exc_send_email(send_error_report_email), 'cron', hour='*/1', id='send_error_report_email',
                 max_instances=10)
schedule.add_job(on_exc_send_email(get_near_city), 'cron', hour='1', id='get_near_city',
                 max_instances=1)
# schedule.add_job(on_exc_send_email(start_update_hotel_validation), 'cron', hour='2', id='update_hotel_validation',
#                  max_instances=1)
schedule.add_job(on_exc_send_email(get_task_info), 'cron', hour='3', id='insert_poi_detect_task_info',
                 max_instances=1)
schedule.add_job(on_exc_send_email(delete_already_scanned_file), 'cron', hour='*/2', id='delete_already_scanned_file',
                 max_instances=1)
schedule.add_job(on_exc_send_email(poi_merge_report_total), 'cron', minute='*/30', id='poi_merge_report_total',
                 max_instances=1)
schedule.add_job(on_exc_send_email(task_progress_report_split_task_main), 'cron', minute='*/5',
                 id='task_progress_report_split_task',
                 max_instances=1)

# 添加 job store
client = pymongo.MongoClient(host='10.10.231.105', port=27017)
store = MongoDBJobStore(client=client, database="SchedulerJob", collection="data_report_jobs")
schedule.add_jobstore(store, 'mongo')

if __name__ == '__main__':
    schedule.start()
    try:
        while True:
            time.sleep(2)
            logger.debug('[main thread sleep][waiting for task]')
    except (KeyboardInterrupt, SystemExit):
        schedule.shutdown()
