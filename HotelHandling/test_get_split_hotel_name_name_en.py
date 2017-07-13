#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/10 下午5:17
# @Author  : Hou Rong
# @Site    : ${SITE}
# @File    : test_get_split_hotel_name_name_en.py
from unittest import TestCase
from HotelHandling.booking_hotel_handling import get_split_hotel_name_name_en
import HotelHandling.expedia_hotel_name_handling


# @Software: PyCharm
class TestGet_split_hotel_name_name_en(TestCase):
    def test_eq(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('思泊客 台北101')
        self.assertEqual(hotel_name, '思泊客 台北101')
        self.assertEqual(hotel_name_en, '')

    def test_eq_2(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('B&B 2 Zen détente')
        self.assertEqual(hotel_name, 'B&B 2 Zen détente')
        self.assertEqual(hotel_name_en, 'B&B 2 Zen détente')

    def test_eq_3(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('United 21- Paradise')
        self.assertEqual(hotel_name, 'United 21- Paradise')
        self.assertEqual(hotel_name_en, 'United 21- Paradise')

    def test_eq_4(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('香港华大盛品酒店 - 贝斯特韦斯特酒店成员')
        self.assertEqual(hotel_name, '香港华大盛品酒店 - 贝斯特韦斯特酒店成员')
        self.assertEqual(hotel_name_en, '')

    def test_eq_5(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('Nosso Hotel (Adult Only)（诺索酒店（仅限成人））')
        self.assertEqual(hotel_name, '诺索酒店（仅限成人）')
        self.assertEqual(hotel_name_en, 'Nosso Hotel (Adult Only)')

    def test_eq_6(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('莫泰168（青岛华阳路台东步行街店 ')
        self.assertEqual(hotel_name, '莫泰168（青岛华阳路台东步行街店 ')
        self.assertEqual(hotel_name_en, '')

    def test_eq_7(self):
        hotel_name, hotel_name_en = get_split_hotel_name_name_en('佳捷连锁酒店（海口保税区店)')
        self.assertEqual(hotel_name, '佳捷连锁酒店（海口保税区店)')
        self.assertEqual(hotel_name_en, '')


class TestGet_expedia_split_hotel_name_name_en(TestCase):
    def test_eq_1(self):
        name, name_en = HotelHandling.expedia_hotel_name_handling.get_split_hotel_name_name_en('法斯特家酒店Hotel Fasthome')
        self.assertEqual(name, '法斯特家酒店')
        self.assertEqual(name_en, 'Hotel Fasthome')

    def test_eq_2(self):
        name, name_en = HotelHandling.expedia_hotel_name_handling.get_split_hotel_name_name_en(
            '納斯拉水療渡假酒店 - 全包式Nashira Resort Hotel & Aqua - Spa - All Inclusive')
        self.assertEqual(name, '納斯拉水療渡假酒店 - 全包式')
        self.assertEqual(name_en, 'Nashira Resort Hotel & Aqua - Spa - All Inclusive')

    def test_eq_3(self):
        name, name_en = HotelHandling.expedia_hotel_name_handling.get_split_hotel_name_name_en(
            '漢庭酒店 (大連黃河路店)Hanting Express')
        self.assertEqual(name, '漢庭酒店 (大連黃河路店)')
        self.assertEqual(name_en, 'Hanting Express')

    def test_eq_4(self):
        name, name_en = HotelHandling.expedia_hotel_name_handling.get_split_hotel_name_name_en(
            'Beautiful Chalet in a Lovely Location and Nearby Thermae 2000')
        self.assertEqual(name, 'Beautiful Chalet in a Lovely Location and Nearby Thermae 2000')
        self.assertEqual(name_en, 'Beautiful Chalet in a Lovely Location and Nearby Thermae 2000')
