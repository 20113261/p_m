#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/10 下午9:14
# @Author  : Hou Rong
# @Site    : 
# @File    : hotels_hotel_handling.py
# @Software: PyCharm

import dataset
import re

db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/hotel_adding?charset=utf8')
zh_pattern = re.compile(u"[\u4e00-\u9fa5]+")


def is_part_legal(string):
    part = string.strip()
    if part:
        if part not in ('-',):
            if not part.isdigit():
                return True
    return False


if __name__ == '__main__':
    _count = 0
    for line in db.query('''SELECT
  source,
  source_id,
  hotel_name,
  hotel_name_en
FROM hotelinfo_static_data WHERE source='hotels' AND hotel_name like '%，%';'''):
        try:
            hotel_name = line['hotel_name'].split('，')[0]
            if len(zh_pattern.findall(hotel_name)) > 0:
                hotel_name_en = 'NULL'
            else:
                hotel_name_en = hotel_name
            db['hotelinfo_static_data'].update(
                row={
                    'hotel_name': hotel_name,
                    'hotel_name_en': hotel_name_en,
                    'source': line['source'],
                    'source_id': line['source_id']
                },
                keys=['source', 'source_id']
            )
        except Exception:
            print(line['source'], line['source_id'], line['hotel_name'], line['hotel_name_en'])
        _count += 1
        if _count % 10000 == 0:
            print(_count)
    print(_count)
