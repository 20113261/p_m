#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/9 下午11:04
# @Author  : Hou Rong
# @Site    : 
# @File    : get_hotel_task.py
# @Software: PyCharm
import json
import logging
import os
import subprocess

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from requests.auth import HTTPBasicAuth

# if google drive remove finished should be used
# from remove_finished import remove_finished

schedule = BlockingScheduler()


def get_offline_worker():
    proc = subprocess.Popen(
        ['/bin/bash', '/root/data/PycharmProjects/celery_using/check_status.sh'],
        stdout=subprocess.PIPE)

    result = []
    for line in proc.stdout:
        string = line.decode()
        if 'host_list_start' in string:
            result = string.split('|')[1:-1]
    return result


@schedule.scheduled_job('cron', second='*/10')
def get_status():
    target_url = 'http://10.10.189.213:15672/api/queues/celery'
    page = requests.get(target_url, auth=HTTPBasicAuth('hourong', '1220'))
    content = page.text
    j_data = json.loads(content)

    # celery message bool
    celery_data = list(filter(lambda x: 'hotel_task' == x['name'], j_data))[0]
    message_count = celery_data['messages']

    if int(message_count) <= 9000:
        os.system('sh /root/data/PycharmProjects/celery_using/init_task.sh')
    else:
        logging.warning('NOW COUNT ' + str(message_count))


if __name__ == '__main__':
    schedule.start()
