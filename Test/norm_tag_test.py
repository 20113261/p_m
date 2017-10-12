#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 下午12:21
# @Author  : Hou Rong
# @Site    : 
# @File    : norm_tag_test.py
# @Software: PyCharm
from norm_tag.attr_norm_tag import get_norm_tag as attr_get_norm_tag
from norm_tag.rest_norm_tag import get_norm_tag as rest_get_norm_tag
from norm_tag.shop_norm_tag import get_norm_tag as shop_get_norm_tag

if __name__ == '__main__':
    case = '牛排馆, 烧烤, 阿根廷菜, 南美风味'
    print(rest_get_norm_tag(case))
