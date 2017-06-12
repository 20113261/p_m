#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 下午3:25
# @Author  : Hou Rong
# @Site    : 
# @File    : similar_common.py
# @Software: PyCharm

import uuid
import pymysql
from collections import defaultdict


class SimilarObject(object):
    def __init__(self, mid, sort_key):
        self.id = mid
        self.similar_keys = set()
        self.sort_key = sort_key

    def append_similar_keys(self, key):
        key = key.strip().lower()
        if key not in ('', 'null', '0'):
            self.similar_keys.add(key)

    def extend_similar_keys(self, keys, separator='|'):
        for key in keys.split('|'):
            self.append_similar_keys(key=key)

    def is_similar(self, similar_object):
        for key in similar_object.similar_keys:
            if key in self.similar_keys:
                return True
        return False

    @staticmethod
    def generate_key():
        return str(uuid.uuid4())

    def __str__(self):
        return '[' + '    '.join([self.id, str(self.similar_keys), str(self.sort_key)]) + ']'

    def __repr__(self):
        return '[' + '    '.join([self.id, str(self.similar_keys), str(self.sort_key)]) + ']'


class SimilarDict(object):
    def __init__(self, sort_keys):
        self.dict = defaultdict(list)
        self.__sort_keys = sort_keys

    def get_similar_key(self, obj):
        for __k, __values in self.dict.items():
            for __value in __values:
                if obj.is_similar(__value):
                    return __k
        return None

    def add_obj(self, obj):
        __key = self.get_similar_key(obj)
        if __key is None:
            __key = obj.generate_key()
        # todo 增加同名判断方法
        self.dict[__key].append(obj)

    def log(self):
        for k, values in self.dict.items():
            if len(values) > 1:
                sorted_values = sorted(values, key=sort_keys, reverse=True)
                print('        '.join(map(lambda x: str(x), sorted_values)))


# 排序依据
def sort_keys(x):
    first_key = 100 if x.sort_key[0] == 'Open' else 1
    return first_key, x.sort_key[1]


__sql_dict = {
    'host': '10.10.68.103',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

if __name__ == '__main__':
    similar_dict = SimilarDict(sort_keys=sort_keys)
    conn = pymysql.connect(**__sql_dict)
    city_id = '10001'
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

        similarObj.append_similar_keys(key=name)
        similarObj.append_similar_keys(key=name_en)
        similarObj.extend_similar_keys(keys=alias)
        similarObj.extend_similar_keys(keys=name_en_alias)

        similar_dict.add_obj(similarObj)

    similar_dict.log()
