#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/10 下午4:26
# @Author  : Hou Rong
# @Site    : 
# @File    : get_hotel_failed_task.py
# @Software: PyCharm
import dataset
import json

db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/Task?charset=utf8')

if __name__ == '__main__':
    #     for line in db.query('''SELECT args
    # FROM Task
    # WHERE finished = 0 ORDER BY rand() LIMIT 10;'''):
    #         print(line['args'])

    for line in db.query('''SELECT args
    FROM Task
    WHERE finished = 0;'''):
        j_data = json.loads(line['args'])
        print(j_data['source'], j_data['other_info']['source_id'], j_data['hotel_url'])
