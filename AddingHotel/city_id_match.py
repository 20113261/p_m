#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/31 下午6:29
# @Author  : Hou Rong
# @Site    : 
# @File    : city_id_match.py
# @Software: PyCharm
import pymysql
from Common.MiojiSimilarCityDict import MiojiSimilarCityDict

if __name__ == '__main__':
    city_dict = MiojiSimilarCityDict()
    conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='chenxiang')
    cursor = conn.cursor()
    cursor.execute('''SELECT
  DISTINCT 
  country,
  city
FROM hotelinfo_dotw_new;''')
    for country, city in cursor.fetchall():
        city_id = city_dict.get_mioji_city_id((country.lower(), city.lower()))
        print(cursor.execute('''UPDATE hotelinfo_dotw_new
SET city_id = %s
WHERE country = %s AND city = %s''', (city_id, country, city)))
