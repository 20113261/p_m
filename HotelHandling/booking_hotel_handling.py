#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/10 下午4:52
# @Author  : Hou Rong
# @Site    : 
# @File    : booking_hotel_handling.py
# @Software: PyCharm
import dataset
import re

db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/hotel_adding?charset=utf8')

zh_pattern_old = re.compile(u"[\u4e00-\u9fa5\- \d]+")

zh_pattern = re.compile(u"[\u4e00-\u9fa5]+")


def is_part_legal(string):
    part = string.strip()
    if part:
        if part not in ('-',):
            if not part.isdigit():
                return True
    return False


def get_split_hotel_name_name_en_old(string):
    zh_part_list = zh_pattern.findall(string)
    if len(zh_part_list) == 1:
        if is_part_legal(zh_part_list[0]):
            if zh_part_list[0].strip() == string.strip():
                hotel_name = zh_part_list[0]
                hotel_name_en = ''
            else:
                hotel_name = re.findall('（([\s\S]+?)）', string)[0]
                hotel_name_en = re.sub('(（[\s\S]+?）)', '', string)
        else:
            hotel_name = hotel_name_en = string
    else:
        hotel_name = hotel_name_en = string
    return hotel_name, hotel_name_en


def get_split_hotel_name_name_en(string):
    if '（' in string and '）' in string:
        hotel_name = re.findall('（([\s\S]+)）', string)[0]
        hotel_name_en = re.sub('(（[\s\S]+）)', '', string)
    elif len(zh_pattern.findall(string)) > 0:
        hotel_name = string
        hotel_name_en = ''
    else:
        hotel_name = hotel_name_en = string
    return hotel_name, hotel_name_en


if __name__ == '__main__':
    _count = 0
    for line in db.query('''SELECT
  source,
  source_id,
  hotel_name,
  hotel_name_en
FROM hotelinfo_booking_new_name_0710 WHERE hotel_name=hotel_name_en;'''):
        try:
            hotel_name, hotel_name_en = get_split_hotel_name_name_en(line['hotel_name'])
            db['hotelinfo_booking_new_name_0710'].update(
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
