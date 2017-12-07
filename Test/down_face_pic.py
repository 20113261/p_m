#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/7 下午12:04
# @Author  : Hou Rong
# @Site    : 
# @File    : down_face_pic.py
# @Software: PyCharm
import pandas
from service_platform_conn_pool import base_data_final_pool

file_path = "/tmp"
csv_name = "attr_img_scan_result.csv"
columns = ["file_name", "source", "sid", "pic_size", "url", "height", "width", "date"]
table = pandas.read_csv(
    "{}/{}".format(file_path, csv_name),
    header=0,
    usecols=columns,
    parse_dates=["date"]
)
new_table = pandas.DataFrame()
for key in columns:
    new_table[key] = table[key]


def update_db(data):
    sql = '''UPDATE poi_images
SET `use` = 0, info = JSON_SET(CASE WHEN info IS NULL
  THEN '{}'
                               ELSE info END, '$.down_reason', '疑似头像，下线')
WHERE file_name = %s;'''
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    _res = cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    print("[total: {}][insert: {}]".format(len(data), _res))


data = []
_count = 0
for i in range(len(new_table)):
    line = dict(new_table.iloc[i])
    if line['height'] <= 640:
        _count += 1
        print(line['file_name'], line['pic_size'])
        data.append((line['file_name'],))

        if len(data) >= 2000:
            print(_count)
            update_db(data)
            data = []

if data:
    print(_count)
    update_db(data)
