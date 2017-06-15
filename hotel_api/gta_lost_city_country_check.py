#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/15 上午11:02
# @Author  : Hou Rong
# @Site    : 
# @File    : gta_lost_city_country_check.py
# @Software: PyCharm
from Common.MiojiSimilarCountryDict import MiojiSimilarCountryDict, is_legal, key_modify
import dataset

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://hourong:hourong@localhost/hotel_api?charset=utf8')
    mioji_similar_dict = MiojiSimilarCountryDict()

    table = db.query('''SELECT DISTINCT
  country_code,
  country_name
FROM gta_city
WHERE miaoji_city_id = '';''')

    matched_count = 0
    for line in table:
        country_id = None
        country_id = mioji_similar_dict.get_mioji_country_id(key_modify(line['country_code']))
        if country_id is None:
            country_id = mioji_similar_dict.get_mioji_country_id(key_modify(line['country_name']))

        if country_id is not None:
            matched_count += 1

        else:
            print(line['country_code'], line['country_name'])
    print(matched_count)
