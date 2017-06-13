#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 下午5:34
# @Author  : Hou Rong
# @Site    : 
# @File    : gta_city_info.py
# @Software: PyCharm
import dataset

CITY_KEYS = ['name', 'name_en']
CITY_MULTI_KEYS = ['alias']
COUNTRY_KEYS = ['country_name', 'country_name_en', 'country_short_name_cn', 'country_short_name_en']
COUNTRY_MULTI_KEYS = ['country_alias']


def is_legal(s):
    if s:
        if s.strip():
            if s.lower() != 'null':
                return True
    return False


def key_modify(s: str):
    return s.strip().lower()


class MiojiSimilarCityDict(object):
    def __init__(self):
        self.dict = self.get_mioji_similar_dict()

    @staticmethod
    def get_keys(__line):
        country_key_set = set()
        city_key_set = set()
        for key in COUNTRY_KEYS:
            if is_legal(__line[key]):
                country_key_set.add(key_modify(__line[key]))

        for key in COUNTRY_MULTI_KEYS:
            if __line[key]:
                for word in __line[key].strip().split('|'):
                    if is_legal(word):
                        country_key_set.add(key_modify(word))

        for key in CITY_KEYS:
            if is_legal(__line[key]):
                city_key_set.add(key_modify(__line[key]))

        for key in CITY_MULTI_KEYS:
            if __line[key]:
                for word in __line[key].strip().split('|'):
                    if is_legal(word):
                        city_key_set.add(key_modify(word))

        for country in country_key_set:
            for city in city_key_set:
                yield country, city

    def get_mioji_similar_dict(self):
        __dict = dict()
        db_test = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
        city_country_info = [
            i for i in db_test.query('''SELECT
      city.id,
      city.name,
      city.name_en,
      city.alias,
      city.tri_code,
      country.country_code,
      country.name          AS country_name,
      country.name_en       AS country_name_en,
      country.alias         AS country_alias,
      country.short_name_cn AS country_short_name_cn,
      country.short_name_en AS country_short_name_en
    FROM city
      JOIN country ON city.country_id = country.mid;''')
        ]
        for __line in city_country_info:
            for key in self.get_keys(__line):
                __dict[key] = __line['id']

        return __dict

    def get_mioji_city_id(self, keys):
        if keys in self.dict:
            return self.dict[keys]
        else:
            return None


if __name__ == '__main__':
    mioji_similar_dict = MiojiSimilarCityDict()
    db = dataset.connect('mysql+pymysql://hourong:hourong@localhost/hotel_api?charset=utf8')
    table = db['gta_city']

    for line in table:
        city_id = None
        # 按国家二字码和城市名匹配
        if is_legal(line['country_code']):
            city_id = mioji_similar_dict.get_mioji_city_id(
                (key_modify(line['country_code']), key_modify(line['city_name'])))

        # 按国家名和城市名进行匹配
        if city_id is None:
            if is_legal(line['country_name']):
                city_id = mioji_similar_dict.get_mioji_city_id(
                    (key_modify(line['country_name']), key_modify(line['city_name'])))

        if city_id is not None:
            print(table.update(
                {
                    'city_code': line['city_code'],
                    'miaoji_city_id': city_id
                },
                keys=['city_code']
            ))
