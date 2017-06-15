#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 下午5:34
# @Author  : Hou Rong
# @Site    : 
# @File    : gta_city_info.py
# @Software: PyCharm
import dataset
import Common.MiojiSimilarCityDict
from Common.MiojiSimilarCityDict import is_legal, key_modify

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

    mioji_similar_dict = Common.MiojiSimilarCityDict.MiojiSimilarCityDict()
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
