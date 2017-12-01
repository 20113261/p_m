#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 上午11:05
# @Author  : Hou Rong
# @Site    : 
# @File    : get_error_city_info.py
# @Software: PyCharm
import re
from collections import defaultdict

cid_set = defaultdict(set)


def main():
    f = open('/search/hourong/no_cid_hotel.sql')
    _count = 0
    for line in f:
        _count += 1
        print(_count)
        for each in eval(re.findall('in (\([\s\S]+\))', line)[0]):
            cid_set[each[0]].add(each[-1])


def get_query_sql():
    for source, cids in cid_set.items():
        sugg_sql = '''SELECT *
FROM hotel_suggestions_city
WHERE source = '{}' AND select_index != -1 AND city_id IN ({});'''. \
            format(source, ','.join(map(lambda x: "'{}'".format(x), cids)))
        print(sugg_sql)

        ota_sql = '''SELECT *
FROM ota_location
WHERE source = '{}' AND city_id IN ({})'''.format(source, ','.join(map(lambda x: "'{}'".format(x), cids)))
        print(ota_sql)


if __name__ == '__main__':
    main()
    get_query_sql()
