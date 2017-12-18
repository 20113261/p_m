#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/18 下午1:56
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_crawl_report.py
# @Software: PyCharm
import pandas
from sqlalchemy.engine import create_engine


def generate_report():
    sql = '''SELECT
  city_id,
  city.name,
  city.name_en,
  source,
  count(*)
FROM hotel_final_20171214a
  JOIN base_data.city ON hotel_final_20171214a.city_id = base_data.city.id
GROUP BY city_id, source;'''
    engine = create_engine('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/BaseDataFinal?charset=utf8')
    table = pandas.read_sql(sql=sql, con=engine)
    new_table = None
    count_key_set = set()
    for key, sub_table in table.groupby(by=['source']):
        this_sub_table = sub_table.copy()
        del this_sub_table['source']
        count_key = '{}_num'.format(key)
        this_sub_table[count_key] = this_sub_table['count(*)']
        del this_sub_table['count(*)']
        count_key_set.add(count_key)

        if new_table is None:
            new_table = this_sub_table
        else:
            new_table = new_table.merge(this_sub_table, how='outer', on='city_id')

    new_table.fillna(0, inplace=True)
    new_table['total'] = sum([new_table[count_key] for count_key in count_key_set])
    return new_table


if __name__ == '__main__':
    table = generate_report()
    print(table)
