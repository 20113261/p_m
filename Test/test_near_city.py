#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 下午12:50
# @Author  : Hou Rong
# @Site    : 
# @File    : test_near_city.py
# @Software: PyCharm
from get_near_city.get_near_city import get_nearby_city

if __name__ == '__main__':
    city_id = '51513'
    map_info = '57.581802,-20.008942'
    nearby_city = get_nearby_city(city_id, map_info)
    print(nearby_city)
