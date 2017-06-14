#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/14 下午4:19
# @Author  : Hou Rong
# @Site    : 
# @File    : CityInfoDict.py
# @Software: PyCharm
import dataset

KEY_TYPE = 'str'


class CityInfoDict(object):
    def __init__(self):
        self.db = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
        self.table = self.db.query('''SELECT
  city.*,
  country.name    AS country_name,
  country.name_en AS country_name_en
FROM city
  JOIN country ON city.country_id = country.mid;''')
        self.dict = {str(line['id']) if KEY_TYPE == 'str' else line['id']: line for line in self.table}
