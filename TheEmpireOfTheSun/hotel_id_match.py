#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/5 下午3:52
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_id_match.py
# @Software: PyCharm

import pandas
import dataset

if __name__ == '__main__':
    # todo 开罗入库
    # db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/private_data?charset=utf8')
    # db_table = db['the_empire_of_the_sun_1']
    # table = pandas.read_csv('/Users/hourong/Downloads/太阳帝国酒店信息/私有酒店&系统酒店 参照表/开罗-表格 1.csv').fillna('')
    #
    # has_name = False
    # has_name_en = False
    # print('#' * 100)
    # per_data = {
    #
    # }
    # city = '开罗'
    # for i in range(len(table)):
    #     line = table.iloc[i]
    #     key_name = line['key_name']
    #     if key_name not in ('英文名', '中文名'):
    #         continue
    #     key_empire = line['empire'].strip()
    #     key_mioji = line['mioji'].strip()
    #     if key_name == '英文名':
    #         has_name_en = True
    #
    #     if key_name == '中文名':
    #         has_name = True
    #
    #     print(key_name, city, key_empire, key_mioji)
    #     if key_name == '中文名':
    #         per_data['empire_name'] = key_empire
    #         per_data['mioji_name'] = key_mioji
    #
    #     if key_name == '英文名':
    #         per_data['empire_name_en'] = key_empire
    #         per_data['mioji_name_en'] = key_mioji
    #
    #     if has_name and has_name_en:
    #         print('#' * 100)
    #         has_name = False
    #         has_name_en = False
    #
    #         per_data['city'] = city
    #         db_table.insert(per_data)
    #         print(per_data)
    #         per_data = {}

    # todo 其他城市入库
    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/private_data?charset=utf8')
    db_table = db['the_empire_of_the_sun_2']
    table = pandas.read_csv('/Users/hourong/Downloads/太阳帝国酒店信息/私有酒店&系统酒店 参照表/Abu Simbel&Asw-表格 1.csv').fillna('')

    has_name = False
    has_name_en = False
    print('#' * 100)
    per_data = {

    }
    city = None
    for i in range(len(table)):
        line = table.iloc[i]
        key_name = line['key_name']
        perhaps_city = line['city']
        if str(perhaps_city) != '':
            if not str(perhaps_city).isdigit():
                city = perhaps_city

        if key_name not in ('英文名', '中文名'):
            continue
        key_empire = line['empire'].strip()
        key_mioji = line['mioji'].strip()
        if key_name == '英文名':
            has_name_en = True

        if key_name == '中文名':
            has_name = True

        print(key_name, city, key_empire, key_mioji)
        if key_name == '中文名':
            per_data['empire_name'] = key_empire
            per_data['mioji_name'] = key_mioji

        if key_name == '英文名':
            per_data['empire_name_en'] = key_empire
            per_data['mioji_name_en'] = key_mioji

        if has_name and has_name_en:
            print('#' * 100)
            has_name = False
            has_name_en = False

            per_data['city'] = city
            db_table.insert(per_data)
            print(per_data)
            per_data = {}
