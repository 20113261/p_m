#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 下午3:15
# @Author  : Hou Rong
# @Site    : 
# @File    : update_tag_id.py
# @Software: PyCharm
import pymysql
from toolbox.Common import is_legal
from Config.settings import dev_conf

poi_type = 'rest'
if poi_type == 'attr':
    tag_s = 'chat_attraction_tagS'
    task_table = 'chat_attraction'
elif poi_type == 'shop':
    tag_s = 'chat_shopping_tagS'
    task_table = 'chat_shopping'
elif poi_type == 'rest':
    tag_s = 'chat_restaurant_tagS'
    task_table = 'chat_restaurant'
else:
    raise TypeError("Unknown Type: {}".format(poi_type))


def tag2id():
    conn = pymysql.connect(**dev_conf)
    sql = '''SELECT
  id,
  tag
FROM {};'''.format(tag_s)
    cursor = conn.cursor()
    cursor.execute(sql)
    _dict = {}
    for _id, _tag_name in cursor.fetchall():
        _dict[_tag_name] = str(_id)
    cursor.close()
    conn.close()
    return _dict


def update_tag_id():
    tag_id = tag2id()
    conn = pymysql.connect(**{
        'host': '10.10.180.145',
        'user': 'hourong',
        'password': 'hourong',
        'charset': 'utf8',
        'db': 'data_prepare'
    })
    cursor = conn.cursor()
    cursor.execute('''SELECT id,norm_tagid
FROM {}
WHERE city_id IN (51502, 51503, 51504, 51505, 51506, 51507, 51508, 51509, 51510, 51511, 51512, 51513, 51514, 51515, 30077, 30160, 40028, 40037, 30109, 40042, 10111, 50739, 10639, 10165);'''.format(
        task_table))
    data = []
    for _id, _tag_id in cursor.fetchall():
        if is_legal(_tag_id):
            tag_id_set = set()
            for each in _tag_id.split('|'):
                tag_id_set.add(tag_id[each])
            data.append(('|'.join(tag_id_set), _id))
    print(cursor.executemany('update test_base_data.{} set tag_id=%s where id=%s'.format(task_table), data))
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    update_tag_id()
