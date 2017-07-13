#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/13 上午10:39
# @Author  : Hou Rong
# @Site    : 
# @File    : expedia_hotel_name_handling.py
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


def get_split_hotel_name_name_en(string: str):
    # 寻找中文字符串
    re_result = re.findall(u"[\u4e00-\u9fa5]+", string)
    name = None
    name_en = None


    # 找到最有一个中文
    if re_result:
        string_result = string.split(re_result[-1])
        # 并且存在英文
        if len(string_result) == 2:
            # 通过中文最后一个字分割，生成中文英文字段
            name_en = string_result[-1].strip()
            name = string.replace(name_en, '').strip()

            # 对多余的括号进行处理
            if name_en.startswith(')'):
                name_en = name_en.replace(')', '')
                name += ')'

    if not (name and name_en):
        name = name_en = string

    return name, name_en


if __name__ == '__main__':
    _count = 0
    for line in db.query('''SELECT
  source,
  source_id,
  hotel_name,
  hotel_name_en
FROM hotelinfo_static_data WHERE hotel_name=hotel_name_en;'''):
        try:
            hotel_name, hotel_name_en = get_split_hotel_name_name_en(line['hotel_name'])
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
            print(line['source'], line['source_id'], line['hotel_name'])
        _count += 1
        if _count % 10000 == 0:
            print(_count)
    print(_count)
