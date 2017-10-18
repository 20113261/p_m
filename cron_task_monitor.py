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
from logging import getLogger, StreamHandler, FileHandler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from service_platform_report.task_progress_report_mongo import main as task_progress_report_mongo
from service_platform_report.task_progress_report import main as task_progress_report
from service_platform_report.crawl_data_check_script import detectOriData
from service_platform_report.data_coverage import data_coverage
from serviceplatform_data.create_view import create_all_view
from serviceplatform_data.insert_final_data import insert_data as detail_insert_data
from serviceplatform_data.image_insert_final_data import insert_data as image_insert_data
from serviceplatform_data.load_final_data import main as load_final_data
from serviceplatform_data.load_final_data_test import main as load_final_data_qyer
from service_platform_report.routine_report import main as routine_report
from logger import get_logger

SEND_TO = ['hourong@mioji.com']

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


def test_exc():
    raise Exception()


'''
#1 * * * * /usr/local/bin/python3 /search/hourong/PycharmProjects/PoiCommonScript/service_platform_report/routine_report.py >> /root/data/PycharmProjects/PoiCommonScript/service_platform_report/task_routine_log
'''

schedule = BackgroundScheduler()
schedule.add_job(on_exc_send_email(task_progress_report_mongo), 'cron', hour='*/2', id='task_progress_report_mongo',
                 max_instances=10)
schedule.add_job(on_exc_send_email(task_progress_report), 'cron', hour='*/2', id='task_progress_report',
                 max_instances=10)
schedule.add_job(on_exc_send_email(detectOriData), 'cron', hour='*/2', id='detectOriData', max_instances=10)
schedule.add_job(on_exc_send_email(data_coverage), 'cron', hour='*/2', id='data_coverage')
# schedule.add_job(on_exc_send_email(detail_insert_final_data), 'cron', minute='*/2', id='detail_insert_final_data')
schedule.add_job(on_exc_send_email(image_insert_final_data), 'cron', minute='*/2', id='image_insert_final_data')
# schedule.add_job(on_exc_send_email(load_final_data), 'cron', minute='*/1', id='load_final_data')
schedule.add_job(on_exc_send_email(routine_report), 'cron', hour='*/1', id='routine_report', max_instances=10)
# schedule.add_job(on_exc_send_email(load_final_data_qyer), 'cron', second='*/20', id='routine_report_qyer')

if __name__ == '__main__':
    schedule.start()
    try:
        while True:
            time.sleep(2)
            logger.debug('[main thread sleep][waiting for task]')
    except (KeyboardInterrupt, SystemExit):
        schedule.shutdown()
