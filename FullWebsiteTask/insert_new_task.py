#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/9 下午10:39
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_new_task.py
# @Software: PyCharm
import pymongo
import dataset
import datetime

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Task']['FullSite']

db_src = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
if __name__ == '__main__':
    for line in db_src.query('''SELECT
  id,
  website_url
FROM chat_attraction
WHERE website_url != '' AND data_source LIKE '%daodao%';'''):
        collections.save({
            'mid': line['id'],
            'website_url': line['website_url'],
            'select_time': datetime.datetime.now()
        })
