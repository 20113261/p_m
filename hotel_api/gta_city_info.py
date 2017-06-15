#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 下午5:34
# @Author  : Hou Rong
# @Site    : 
# @File    : gta_city_info.py
# @Software: PyCharm
import dataset
from Common.MiojiSimilarCityDict import MiojiSimilarCityDict, is_legal, key_modify

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
