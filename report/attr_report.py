#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/12 下午2:23
# @Author  : Hou Rong
# @Site    : 
# @File    : attr_report.py
# @Software: PyCharm
import pandas

table_zero_map_info = pandas.read_csv('/Users/hourong/Downloads/经纬度 0 影响的城市.csv')
table_zero_map_info.columns = ['city_id', 'zero_map_info_country', 'zero_map_info_name', 'zero_map_info_num']
table_zero_map_info.fillna(0)
table_null_attr = pandas.read_csv('/Users/hourong/Downloads/非景点数据影响的城市.csv')
table_null_attr.columns = ['city_id', 'null_attr_country', 'null_attr_name', 'null_attr_num']
table_null_attr.fillna(0)
table_576 = pandas.read_csv('/Users/hourong/Downloads/576 条影响的城市.csv')
table_576.columns = ['city_id', '576_country', '576_name', '576_num']
table_576.fillna(0)
table_human_read = pandas.read_csv(
    '/Users/hourong/Downloads/穷游景点抓取数量.csv',
    usecols=['city_id', 'name', 'name.1', 'name.2', 'E-F'],
)
table_human_read.columns = ['city_id', 'country', 'region', 'city', 'diff']
table_human_read = table_human_read[table_human_read['diff'] != 0]

res_table = table_zero_map_info. \
    merge(table_null_attr, how='outer', on='city_id'). \
    merge(table_576, how='outer', on='city_id'). \
    merge(table_human_read, how='outer', on='city_id')

table_576.fillna(0, inplace=True)
res_table['total'] = res_table['zero_map_info_num'].fillna(0) + res_table['null_attr_num'].fillna(0) + res_table[
    '576_num'].fillna(0)

available_cid = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010, 10011, 10012, 10013, 10014,
                 10015, 10016, 10017, 10018, 10019, 10020, 20045, 20046, 20047, 20048, 20049, 20050, 20052, 20053,
                 20054, 20055, 20056, 20057, 20058, 20059, 20061, 20062, 20064, 20065, 20066, 20067, 30001, 30002,
                 30003, 30005, 30010, 30014, 30015, 30018, 30019, 30020, 30022, 30023, 30025, 30026, 30030, 30031,
                 30032, 30033, 30035, 30036, 40001, 40002, 40003, 40004, 40005, 40007, 40011, 40012, 40014, 40015,
                 40018, 40020, 40023, 40025, 40028, 40030, 40031, 40032, 40033, 40034, 50001, 50002, 50003, 50004,
                 50005, 50006, 50007, 50009, 50010, 50012, 50013, 50014, 50016, 50017, 50018, 50019, 50020, 50021,
                 50027, 50028]
res_table = res_table[res_table['city_id'].isin(available_cid)].sort_values(by='city_id')
res_table.to_csv('/Users/hourong/Downloads/city_res.csv')
res_table
