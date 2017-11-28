#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/28 上午9:20
# @Author  : Hou Rong
# @Site    : 
# @File    : read_large_xml.py
# @Software: PyCharm
import xml.etree.ElementTree as ET


def get_source():
    tree = ET.iterparse('/search/hourong/1128/hotel_unid_online.xml')
    _count = 0
    for status, element in tree:
        if element.tag == 'row':
            # each row
            line = {}
            for field in element:
                line[field.attrib['name']] = field.text
            yield line
        _count += 1
        if _count == 100:
            break


if __name__ == '__main__':
    for each in get_source():
        print(each)
