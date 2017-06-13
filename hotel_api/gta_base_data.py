#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/12 下午4:36
# @Author  : Hou Rong
# @Site    : 
# @File    : gta_base_data.py
# @Software: PyCharm
import pandas
from sqlalchemy import create_engine

if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://hourong:hourong@localhost/hotel_api?charset=utf8')
    res = pandas.read_excel('/tmp/GTA Properties - 2017-06-01.xlsx')
    res.to_sql('gta_0601', engine)
