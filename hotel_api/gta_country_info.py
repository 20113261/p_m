#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/8 上午10:58
# @Author  : Hou Rong
# @Site    : 
# @File    : gta_country_info.py
# @Software: PyCharm
import pymysql
import Common.MiojiSimilarCountryDict

# Common.MiojiSimilarCountryDict.COUNTRY_KEYS.append('country_code')
Common.MiojiSimilarCountryDict.COUNTRY_KEYS = ['country_name_en', 'country_code']

if __name__ == '__main__':
    conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='hotel_api')

    cursor = conn.cursor()
    similar_dict = Common.MiojiSimilarCountryDict.MiojiSimilarCountryDict()

    cursor.execute('''
SELECT
  DISTINCT
  country_code,
  country_name
FROM gta_city;
''')
    for code, country_name in cursor.fetchall():
        country_id = similar_dict.get_mioji_country_id(country_name.lower())
        if not country_id:
            country_id = similar_dict.get_mioji_country_id(code.lower())
        if not country_id:
            country_id = 'NULL'

        cursor.execute('''UPDATE gta_city
SET country_id = %s
WHERE country_code = %s AND country_name = %s;''', (country_id, code, country_name))
