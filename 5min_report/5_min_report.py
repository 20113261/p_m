#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/28 下午3:11
# @Author  : Hou Rong
# @Site    : 
# @File    : 5_min_report.py
# @Software: PyCharm
import dataset
import datetime
import pymongo

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.238.148/task_db?charset=utf8')
    client = pymongo.MongoClient(host='10.10.231.105')

    collections = client['Report']['5min_report']

    now_datetime = str(int(datetime.datetime.today().strftime('%Y%m%d%H%M')) // 5 * 5)
    for line in db.query('''SELECT *
FROM task_verify_monitor
WHERE date IN (SELECT max(date)
               FROM task_verify_monitor) and source NOT LIKE "S%"'''):

        # 获取基础信息
        date = line['date']
        source = line['source']
        #
        # # 删除本时间旧的信息，保存信息
        collections.delete_one({'datetime': now_datetime, 'source': source, 'date': date})
        collections.save(
            {
                'datetime': now_datetime,
                'source': source,
                'date': date,
                'line': dict(line)
            }
        )

        # 查询旧信息，以便完成统计
        old_data = collections.find_one({'datetime': str(int(now_datetime) - 5), 'date': date, 'source': source})

        if old_data:
            for k, v in old_data['line'].items():
                if k.startswith("_") and k.endswith("_") or k in ('other', 'error_count'):
                    line[k] = old_data['line'][k] - line[k]

        line['source'] = line['source']
        db['task_verify_monitor_5min'].upsert(line, keys=['source', 'date', 'datetime'])
