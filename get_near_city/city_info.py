#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 下午12:50
# @Author  : Hou Rong
# @Site    : 
# @File    : city_info.py
# @Software: PyCharm
import pymysql
from toolbox.Common import is_legal
from Config.settings import dev_conf


def map_info_is_legal(_m_info):
    if is_legal(v):
        try:
            m1, m2 = v.split(',')
            if ' ' in m1 or ' ' in m2:
                raise Exception("Map Info With Space")
            float(m1)
            float(m2)
            return True
        except Exception:
            pass
    return False


conn = pymysql.connect(**dev_conf)
cursor = conn.cursor()
sql = '''SELECT
  id,
  map_info
FROM city;'''
cursor.execute(sql)
cid2map = {}
for k, v in cursor.fetchall():
    if map_info_is_legal(v):
        _m1, _m2 = v.split(',')
        cid2map[str(k)] = (float(_m1), float(_m2))
cursor.close()

cursor = conn.cursor()
sql = '''SELECT id
FROM city
  JOIN country ON city.country_id = country.mid
WHERE country.continent_id IN (30, 50, 60);'''
cursor.execute(sql)
special_city = {str(k) for k in cursor.fetchall()}
cursor.close()
conn.close()
