#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/9 下午11:04
# @Author  : Hou Rong
# @Site    : 
# @File    : get_full_site_spider_task.py
# @Software: PyCharm
import json
import logging
import os
import subprocess

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from requests.auth import HTTPBasicAuth

schedule = BlockingScheduler()


@schedule.scheduled_job('cron', second='*/60cd ')
def get_status():
    target_url = 'http://10.10.189.213:15672/api/queues/celery'
    page = requests.get(target_url, auth=HTTPBasicAuth('hourong', '1220'))
    content = page.text
    j_data = json.loads(content)

    # celery message bool
    celery_data = list(filter(lambda x: 'file_downloader' == x['name'], j_data))[0]
    message_count = celery_data['messages']

    # insert task into queue
    if int(message_count) <= 9000:
        os.system('sh /root/data/PycharmProjects/celery_using/spider_init_by_mongo.sh')
    else:
        logging.warning('NOW COUNT ' + str(message_count))


if __name__ == '__main__':
    schedule.start()
