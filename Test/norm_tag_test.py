#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 下午12:21
# @Author  : Hou Rong
# @Site    : 
# @File    : norm_tag_test.py
# @Software: PyCharm
# from norm_tag.attr_norm_tag import get_norm_tag as attr_get_norm_tag
# from norm_tag.rest_norm_tag import get_norm_tag as rest_get_norm_tag
# from norm_tag.shop_norm_tag import get_norm_tag as shop_get_norm_tag
import unittest
from norm_tag.norm_tag import get_norm_tag


class TestNormTag(unittest.TestCase):
    def test_case_1(self):
        case = '建筑_博物馆_历史遗址_机构_小镇'

        self.assertTupleEqual(get_norm_tag(case, 'attr'),
                              ('博物馆|历史遗址|小镇|建筑|机构', 'Building|Historical Ruins|Museum|Organization|Small Town', []))

    # def test_case_2(self):
    #     # todo 等待 rest 添加后启用
    #     case = '牛排馆, 烧烤, 阿根廷菜, 南美风味，啦啦啦啦'
    #     self.assertTupleEqual(get_norm_tag(case, 'rest'),
    #                           ('', '', ['牛排馆', '烧烤', '阿根廷菜', '南美风味', '啦啦啦啦']))

    def test_case_3(self):
        case = '     雕塑'
        self.assertTupleEqual(get_norm_tag(case, 'attr'),
                              ('雕塑', 'Sculpture', []))

    def test_case_4(self):
        case = '购物，大商场'
        self.assertTupleEqual(get_norm_tag(case, 'shop'),
                              ('', '', ['购物', '大商场']))

    def test_case_5(self):
        case = '小镇_寺庙'
        self.assertTupleEqual(get_norm_tag(case, 'attr'),
                              ('寺庙|小镇', 'Small Town|Temple', []))


if __name__ == '__main__':
    unittest.main()
