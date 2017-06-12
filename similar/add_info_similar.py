#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 下午12:23
# @Author  : Hou Rong
# @Site    : 
# @File    : add_info_similar.py
# @Software: PyCharm
import pymysql
import re
import uuid
from collections import defaultdict

__sql_dict = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'password': 'hourong',
    'charset': 'utf8',
    'db': 'onlinedb'
}


class SimilarObject(object):
    def __init__(self, mid, sort_key):
        self.id = mid
        self.similar_keys = set()
        self.sort_key = sort_key

    def add_similar_keys(self, key):
        key = key.strip().lower()
        if key not in ('', 'null', '0'):
            self.similar_keys.add(key)

    def is_similar(self, similar_object):
        return similar_object.id == self.id

    @staticmethod
    def generate_key():
        return str(uuid.uuid4())

    def __str__(self):
        return '[' + '\t'.join([self.id, str(self.similar_keys), str(self.sort_key)]) + ']'

    def __repr__(self):
        return '[' + '\t'.join([self.id, str(self.similar_keys), str(self.sort_key)]) + ']'


# 遍历字典，判断是否存在相似
def get_similar_key(similar_dict, similarObj):
    for k, values in similar_dict.items():
        for value in values:
            if similarObj.is_similar(value):
                return k
    return None


def group_and_sort(conn, add_info_set, similar_dict):
    cursor = conn.cursor()
    cursor.execute(
        '''SELECT
      id,
      name,
      name_en,
      alias,
      name_en_alias,
      hot,
      status_online
    FROM chat_attraction
    WHERE id IN ({0})'''.format(
            ','.join(
                map(
                    lambda x: '"' + x + '"', add_info_set
                )
            )
        )
    )
    for mid, name, name_en, alias, name_en_alias, hot, status_online in cursor.fetchall():
        # 初始化相似对象
        similarObj = SimilarObject(mid=mid, sort_key=(status_online, hot))

        similarObj.add_similar_keys(key=name)
        similarObj.add_similar_keys(key=name_en)

        for key in alias.split('|'):
            similarObj.add_similar_keys(key=key)

        for key in name_en_alias.split('|'):
            similarObj.add_similar_keys(key=key)

        # 判断相似并生成相似键
        similar_key = get_similar_key(similar_dict, similarObj)
        if similar_key is None:
            similar_key = similarObj.generate_key()

        # 判断是会否有同 id 在 list 中
        if all(map(lambda x: x.id != similarObj.id, similar_dict[similar_key])):
            # 添加相似值
            similar_dict[similar_key].append(similarObj)
    cursor.close()


# 排序依据
def sort_keys(x):
    first_key = 100 if x.sort_key[0] == 'Open' else 1
    return first_key, x.sort_key[1]


if __name__ == '__main__':
    total_similar_list = list()
    conn = pymysql.connect(**__sql_dict)
    cursor = conn.cursor()

    # 获取全量的 add_info 信息
    cursor.execute('''SELECT id, add_info
FROM chat_attraction
WHERE add_info != '';''')

    for mid, add_info in cursor.fetchall():
        add_info_set = set(
            re.findall('v\d{6}', add_info)
        )
        add_info_set.add(mid)
        total_similar_list.append(add_info_set)
    cursor.close()

    # 添加相似字典
    similar_dict = defaultdict(list)
    for value in total_similar_list:
        group_and_sort(conn, value, similar_dict)

    # 判断结束，排序并输出值
    for k, values in similar_dict.items():
        if len(values) > 1:
            sorted_values = sorted(values, key=sort_keys, reverse=True)
            print('\t\t\t'.join(map(lambda x: str(x), sorted_values)))
            # list(map(lambda x: delete_key.add(x.id), sorted_values[1:]))
