#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 上午9:42
# @Author  : Hou Rong
# @Site    : 
# @File    : attraction_similar.py
# @Software: PyCharm

import pymysql
import uuid
from collections import defaultdict

__sql_dict = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'password': 'hourong',
    'charset': 'utf8',
    'db': 'onlinedb'
}

total_delete_keys = list()


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
        for key in similar_object.similar_keys:
            if key in self.similar_keys:
                return True
        return False

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


# 排序依据
def sort_keys(x):
    first_key = 100 if x.sort_key[0] == 'Open' else 1
    return first_key, x.sort_key[1]


def group_and_sort(conn, city_id, country, city_name, city_name_en):
    print('#' * 100)
    print(city_id, country, city_name, city_name_en)
    print()
    similar_dict = defaultdict(list)
    cursor = conn.cursor()
    cursor.execute('''SELECT
      id,
      name,
      name_en,
      alias,
      name_en_alias,
      hot,
      status_online
    FROM chat_attraction
    WHERE city_id = {0};'''.format(city_id))

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

        # 添加相似值
        similar_dict[similar_key].append(similarObj)

    # 判断结束，排序并输出值
    delete_key = set()
    for k, values in similar_dict.items():
        if len(values) > 1:
            sorted_values = sorted(values, key=sort_keys, reverse=True)
            print('\t\t\t'.join(map(lambda x: str(x), sorted_values)))
            list(map(lambda x: delete_key.add(x.id), sorted_values[1:]))

    # 增加总删除项
    total_delete_keys.extend(delete_key)

    # 打印报告
    print()
    print('删除项', str(delete_key))
    cursor.close()
    print('#' * 100)


if __name__ == '__main__':
    city_info_list = []
    conn = pymysql.connect(**__sql_dict)
    cursor = conn.cursor()
    cursor.execute('''SELECT
  id,
  city.name,
  city.name_en,
  country.name
FROM city
  JOIN country ON city.country_id = country.mid;''')
    city_info_list = list(cursor.fetchall())
    cursor.close()

    for city_id, name, name_en, country in city_info_list:
        group_and_sort(conn=conn, city_id=city_id, country=country, city_name=name, city_name_en=name_en)

    delete_id_f = open('/root/data/task/delete_ids', 'w')
    delete_id_f.write('\n'.join(map(lambda x: str(x), total_delete_keys)))
