#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/4 下午7:03
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_suggestion_task.py
# @Software: PyCharm
import dataset
from Common.MiojiSimilarCityDict import is_legal, key_modify
from TaskScheduler.TaskInsert import InsertTask

db = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
if __name__ == '__main__':
    with InsertTask(worker='hotel_suggestion', task_name='ctrip_suggestion_0704') as it:
        for line in db.query('''SELECT
  id,
  name,
  name_en,
  alias
FROM city'''):
            key_set = set()
            if is_legal(line['name']):
                key_set.add(key_modify(line['name']))

            if is_legal(line['name_en']):
                key_set.add(key_modify(line['name_en']))

            for i in line['alias'].split('|'):
                if is_legal(i):
                    key_set.add(key_modify(i))

            for key in key_set:
                it.insert_task({
                    'city_id': line['id'],
                    'keyword': key
                })
