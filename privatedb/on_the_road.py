#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/13 下午2:22
# @Author  : Hou Rong
# @Site    : 
# @File    : on_the_road.py
# @Software: PyCharm
import os
import Common.MiojiSimilarCityDict

if __name__ == '__main__':
    Common.MiojiSimilarCityDict.KEY_TYPE = 'str'
    Common.MiojiSimilarCityDict.STR_KEY_SPLIT_WORD = ''
    mioji_common_city_dict = Common.MiojiSimilarCityDict.MiojiSimilarCityDict()

    for filename in os.listdir('/tmp/在路上欧洲酒店成本数据'):
        country_city = filename.split('-')[0]
        city_id = mioji_common_city_dict.get_mioji_city_id(country_city)

        if city_id is not None:
            print(city_id)
        else:
            print(country_city)
