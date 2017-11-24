#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/24 下午3:26
# @Author  : Hou Rong
# @Site    : 
# @File    : too_long_data_detect.py
# @Software: PyCharm
import pandas
import numpy as np
from sqlalchemy.engine import create_engine

base_data_str = 'mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8'
engine = create_engine(base_data_str)

sql = '''SELECT
  city.id       AS city_id,
  country.name  AS country_name,
  province.name AS prov_name,
  city.name     AS city_name,
  city.map_info AS city_map_info,
  chat_attraction.id,
  chat_attraction.name,
  chat_attraction.name_en,
  chat_attraction.map_info,
  chat_attraction.url
FROM chat_attraction
  JOIN city ON chat_attraction.city_id = city.id
  JOIN country ON city.country_id = country.mid
  LEFT JOIN province ON city.prov_id = province.id LIMIT 10;'''
table = pandas.read_sql(con=engine, sql=sql).fillna("NULL")


def dist_from_coordinates(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km

    # conversion to radians
    d_lat = np.radians(lat2 - lat1)
    d_lon = np.radians(lon2 - lon1)

    r_lat1 = np.radians(lat1)
    r_lat2 = np.radians(lat2)

    # haversine formula
    a = np.sin(d_lat / 2.) ** 2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(d_lon / 2.) ** 2

    haversine = 2 * R * np.arcsin(np.sqrt(a))

    return haversine


def get_distance(c_map_info, p_map_info):
    try:
        c_lon, c_lat = c_map_info.split(',')
        p_lon, p_lat = p_map_info.split(',')
        return dist_from_coordinates(float(c_lat), float(c_lon), float(p_lat), float(p_lon))
    except:
        pass
    return -1


table['distance'] = table.apply(lambda x: get_distance(x['city_map_info'], x['map_info']), axis=1)
table[(table['distance'] >= 100)]
