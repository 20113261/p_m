#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/13 下午2:38
# @Author  : Hou Rong
# @Site    : 
# @File    : MiojiSimilarCountryDict.py
# @Software: PyCharm
import dataset

# 相似多字段分割符
MULTI_SPLIT_KEY = '|'
# 从国家表中获取的字段
COUNTRY_KEYS = ['country_name', 'country_name_en', 'country_short_name_cn', 'country_short_name_en']
# 从国家表中获取的多字段，中间用 MULTI_SPLIT_KEY 分割
COUNTRY_MULTI_KEYS = ['country_alias']


def is_legal(s):
    if s:
        if s.strip():
            if s.lower() != 'null':
                return True
    return False


def key_modify(s: str):
    return s.strip().lower()


class MiojiSimilarCountryDict(object):
    def __init__(self):
        self.dict = self.get_mioji_similar_dict()

    @staticmethod
    def get_keys(__line):
        country_key_set = set()
        for key in COUNTRY_KEYS:
            if is_legal(__line[key]):
                country_key_set.add(key_modify(__line[key]))

        for key in COUNTRY_MULTI_KEYS:
            if __line[key]:
                for word in __line[key].strip().split(MULTI_SPLIT_KEY):
                    if is_legal(word):
                        country_key_set.add(key_modify(word))

        for country in country_key_set:
            yield country

    def get_mioji_similar_dict(self):
        __dict = dict()
        db_test = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
        country_info = [
            i for i in db_test.query('''SELECT
  country.mid            AS id,
  country.name AS country_name,
  country.name_en       AS country_name_en,
  country.alias         AS country_alias,
  country.short_name_cn AS country_short_name_cn,
  country.short_name_en AS country_short_name_en
FROM country''')
        ]
        for __line in country_info:
            for key in self.get_keys(__line):
                __dict[key] = __line['id']

        return __dict

    def get_mioji_country_id(self, keys):
        if keys in self.dict:
            return self.dict[keys]
        else:
            return None


if __name__ == '__main__':
    mioji_similar_dict = MiojiSimilarCountryDict()
    print('Hello World')
