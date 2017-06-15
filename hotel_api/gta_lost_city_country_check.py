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
    Common.MiojiSimilarCityDict.ADDITIONAL_COUNTRY_LIST = {
        '102': 'United Arab Emirates',
        '109': 'South Korea',
        '113': 'Laos',
        '127': 'Brunei Darussalam',
        '225': 'Ireland (Republic of)',
        '229': 'Bosnia Herzegovina',
        '248': 'Macau',
        '405': 'Congo (Democratic Republic)',
        '510': 'Trinidad & Tobago',
        '630': 'San Marino (Republic of)',
        '645': 'Congo (Republic of)',
        '679': 'Antigua and Barbuda',
        '687': 'Saint Martin (French part)',
        '696': 'Virgin Islands (USA)',
        '702': 'Saint Lucia',
        '706': 'Turks and Caicos Islands',
        '729': 'Cook Islands'
    }

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
