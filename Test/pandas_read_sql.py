#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/14 上午11:43
# @Author  : Hou Rong
# @Site    : 
# @File    : pandas_read_sql.py
# @Software: PyCharm
import pandas
from sqlalchemy.engine import create_engine

base_data_str = 'mysql+pymysql://reader:mioji1109@10.10.228.253/poi_merge?charset=utf8'
engine = create_engine(base_data_str)
sql = '''SELECT
  tmp.chat_attraction.id,
  tmp.chat_attraction.name,
  tmp.chat_attraction.name_en,
  tmp.chat_attraction.map_info,
  base_data.city.map_info AS city_map_info,
  tmp.chat_attraction.city_id,
  tmp.chat_attraction.ranking,
  json_extract(url, '$.qyer')
FROM tmp.chat_attraction
  JOIN base_data.city
WHERE tmp.chat_attraction.data_source LIKE '%%qyer%%' AND tmp.chat_attraction.id NOT IN (
  SELECT id
  FROM chat_attraction
  WHERE data_source LIKE '%%qyer%%');'''

table = pandas.read_sql(con=engine, sql=sql).fillna("NULL")
print(table)
