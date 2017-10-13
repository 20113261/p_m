#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/13 下午10:46
# @Author  : Hou Rong
# @Site    : 
# @File    : cron_task_monitor.py
# @Software: PyCharm
import sys
import traceback
import logging
import requests
import functools
from logging import getLogger, StreamHandler, FileHandler
from apscheduler.schedulers.blocking import BlockingScheduler
from service_platform_report.task_progress_report_mongo import main as task_progress_report_mongo
from service_platform_report.task_progress_report import main as task_progress_report
from service_platform_report.crawl_data_check_script import detectOriData
from service_platform_report.data_coverage import data_coverage
from serviceplatform_data.create_view import create_all_view
from serviceplatform_data.insert_final_data import insert_data as detail_insert_data
from serviceplatform_data.image_insert_final_data import insert_data as image_insert_data
from serviceplatform_data.load_final_data import main as load_final_data
from service_platform_report.routine_report import main as routine_report

SEND_TO = ['hourong@mioji.com']

logger = getLogger("load_data")
logger.level = logging.DEBUG
s_handler = StreamHandler()
f_handler = FileHandler(
    filename='/search/log/cron/cron_task_monitor.log'
)
logger.addHandler(s_handler)
logger.addHandler(f_handler)


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
        try:
            func()
            send_email('[异常监控]统计及数据入库例行 执行', '{}'.format(func.__name__, ), SEND_TO)
        except Exception:
            logger.error(traceback.format_exc())
            send_email('[异常监控]统计及数据入库例行 异常', '{} \n {}'.format(func.__name__, traceback.format_exc()), SEND_TO)

    return wrapper


def detail_insert_final_data():
    create_all_view()
    detail_insert_data(limit=5000)


def image_insert_final_data():
    image_insert_data(limit=5000)


def test_exc():
    raise Exception()


schedule = BlockingScheduler()
schedule.add_job(on_exc_send_email(task_progress_report_mongo), 'cron', hour='*/2')
schedule.add_job(on_exc_send_email(task_progress_report), 'cron', hour='*/2')
schedule.add_job(on_exc_send_email(detectOriData), 'cron', hour='*/2')
schedule.add_job(on_exc_send_email(data_coverage), 'cron', hour='*/2')
schedule.add_job(on_exc_send_email(detail_insert_final_data), 'cron', minute='*/5')
schedule.add_job(on_exc_send_email(image_insert_final_data), 'cron', minute='*/3')
schedule.add_job(on_exc_send_email(load_final_data), 'cron', hour='2')
schedule.add_job(on_exc_send_email(routine_report), 'cron', hour='*/1')

if __name__ == '__main__':
    schedule.start()
