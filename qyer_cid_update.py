#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/1/2 下午4:31
# @Author  : Hou Rong
# @Site    : 
# @File    : qyer_cid_update.py
# @Software: PyCharm
import pandas
import re

if __name__ == '__main__':
    table = pandas.read_csv('/tmp/qyer_city_mapping_2.csv')
    for each in table.iterrows():
        line = dict(each[1])
        print('''UPDATE ota_location_qyer_1226
SET city_id = '{}'
WHERE SOURCE = 'qyer' AND sid = '{}';'''.format(line['miojicity'],
                                                re.findall('place.qyer.com/([\s\S]+)/', line['qyerurl'])[0]))
