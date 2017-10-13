#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/10 上午9:26
# @Author  : Hou Rong
# @Site    : 
# @File    : routine_report.py
# @Software: PyCharm
import redis
import datetime
import dataset
from collections import defaultdict


def main():
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    table = db['serviceplatform_routine_task_summary']

    r = redis.Redis(host='10.10.180.145', db=15)
    dt = datetime.datetime.now()
    work_count = defaultdict(int)
    for key in r.keys():
        task_type = 'NULL'
        task_source = 'NULL'
        key_list = key.decode().split('|_||_|')
        if len(key_list) != 6:
            continue
        else:
            worker, local_ip, task_source, task_type, task_error_code, task_name = key_list
        count = r.get(key)

        work_count[(worker, task_name, local_ip, task_source, task_type, task_error_code)] += int(count)

    for key in work_count.keys():
        worker, task_name, local_ip, task_source, task_type, task_error_code = key
        data = {
            'worker_name': worker,
            'task_name': task_name,
            'slave_ip': local_ip,
            'source': task_source,
            'type': task_type,
            'error_code': task_error_code,
            'count': work_count[key],
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            table.insert(data, ensure=None)
            # print(data)
        except Exception:
            pass

        print(worker, task_name, local_ip, task_source, task_type, work_count[key],
              datetime.datetime.strftime(dt, '%Y%m%d'),
              datetime.datetime.strftime(dt, '%H'), datetime.datetime.strftime(dt, '%Y%m%d%H00'))

        r.flushdb()


if __name__ == '__main__':
    main()
