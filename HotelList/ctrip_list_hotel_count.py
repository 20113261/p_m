#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/14 下午2:34
# @Author  : Hou Rong
# @Site    : 
# @File    : ctrip_list_hotel_count.py
# @Software: PyCharm
import pymysql
import json

conn = pymysql.connect(host='10.10.228.253', user='writer', password='miaoji1109', db='hotel_adding', charset='utf8')
cursor = conn.cursor()
if __name__ == '__main__':
    cursor.execute('''SELECT
  city_id,
  suggestions,
  annotation,
  select_index
FROM hotel_suggestions_city
WHERE hotel_suggestions_city.source = 'ctrip' AND annotation != -1 AND
      hotel_suggestions_city.city_id NOT IN (SELECT DISTINCT city_id
                                             FROM hotelinfo_ctrip_new_final)''')
    for line in cursor.fetchall():
        city_id = line[0]
        suggestions = line[1]
        select_index = line[3]
        print(city_id,json.loads(suggestions)[select_index - 1])



