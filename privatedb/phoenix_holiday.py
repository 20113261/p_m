#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/13 下午2:22
# @Author  : Hou Rong
# @Site    : 
# @File    : on_the_road.py
# @Software: PyCharm
import os
import pandas
import dataset
import Common.MiojiSimilarCityDict
import Common.CityInfoDict
import Common.DateRange
from collections import OrderedDict

known_city = {
    # '斯洛文尼亚布拉迪斯拉发': '11526',
    # '法国普罗旺斯': '11103',
    # '荷兰阿姆': '10007',
    # '德国杜塞': '10053',
    # '希腊圣岛': '11520',
    # '德国滴滴湖': '12156',
    # '西班牙萨拉尔萨': '10205',
    # '爱尔兰贝尔法斯特': '10132',
    # '英国温德米尔湖区': '11405',
    # '卢森堡': '10034',

    '布拉格': '10011'
}

PTID = '9so8'
SID = 's100149249so8'
# HOTEL_START_ID = 10002700
# HOTEL_ROOM_START_ID = 10002444
HOTEL_START_ID = 10065960
HOTEL_ROOM_START_ID = 10003440


def get_hotel_info():
    hotel_id = HOTEL_START_ID
    hotel_room_id = HOTEL_ROOM_START_ID
    Common.MiojiSimilarCityDict.ADDITIONAL_COUNTRY_LIST['206'] = 'Switz.'
    mioji_common_city_dict = Common.MiojiSimilarCityDict.MiojiSimilarCityDict()
    city_info_dict = Common.CityInfoDict.CityInfoDict().dict

    file_path = '/Users/hourong/Downloads/凤凰酒店成本核算表.csv'

    table = pandas.read_csv(file_path)
    table = table.fillna('')
    # 去掉 table 中的空行
    new_lines = []
    for i in range(len(table)):
        old_line = table.iloc[i]
        if old_line['星级'] == '' or old_line['距离'] == '':
            continue
        new_lines.append(old_line)

    table = pandas.DataFrame(new_lines)

    for i in range(len(table)):
        # 内容选取
        line = table.iloc[i]
        _country = line['Country']
        _city = line['城市']
        _city_en = line['City']

        city_id = mioji_common_city_dict.get_mioji_city_id((_country.lower(), _city.lower()))
        if not city_id:
            city_id = mioji_common_city_dict.get_mioji_city_id((_country.lower(), _city_en.lower()))
        if not city_id:
            city_id = known_city[_city]

        name = line['星级']
        name_en = line['距离']
        per_price = line['限制价格']
        if per_price == '':
            per_price = -1.0

        # 五星不入库
        if name == '五星':
            continue

        room_type = None
        bed_type = None
        occ = None

        if i % 5 == 0:
            room_type = '标准双人间'
            bed_type = '两张单床'
            occ = 2
            # 开始下一个酒店
            hotel_id += 1

        elif i % 5 == 1:
            room_type = '标准双人间'
            bed_type = '一张大床'
            occ = 2
        elif i % 5 == 2:
            room_type = '三人间'
            bed_type = '不保证床型'
            occ = 3
        elif i % 5 == 3:
            room_type = '标准单人间'
            bed_type = '一张单床'
            occ = 1
        elif i % 5 == 4:
            room_type = '大床单人间'
            bed_type = '一张大床'
            occ = 1

        price = per_price * occ if per_price != '' else -1

        # 自增 hotel_id room_id
        hotel_room_id += 1

        star = None
        if name == '三星':
            star = 3
        elif name == '四星':
            star = 4

        yield {
            # 'id': hotel_id,
            'hotel_name': name,
            'hotel_name_en': name_en,
            # 'uid': 'ht{0}{1}'.format(hotel_id, PTID),
            'city_mid': city_id,
            'city': city_info_dict[city_id]['name'],
            'country': city_info_dict[city_id]['country_name'],
            'map_info': city_info_dict[city_id]['map_info'],
            # 'roomId': 'r{0}{1}'.format(hotel_room_id, PTID),
            # 'sid': SID,
            # 'ptid': PTID,
            'room_type': room_type,
            'bed_type': bed_type,
            'occ': occ,
            'price': float(price),
            'per_price': float(per_price),
            'service': 'breakfast',
            'is_breakfast_free': 'Yes',
            'ccy': "EUR",
            'img_list': '',
            'star': star
        }, i % 5 == 0


if __name__ == '__main__':
    # DateRange Config
    Common.DateRange.DATE_FORMAT = '%Y%m%d'

    for each, need_insert_hotel in get_hotel_info():
        print(each)
        # 初始化数据库对象
        # debug
        # db_insert = dataset.connect('mysql+pymysql://hourong:hourong@localhost/private_data?charset=utf8')
        # test
        # db_insert = dataset.connect('mysql+pymysql://writer:miaoji1109@10.10.43.99/private_data?charset=utf8')
        # online
        # db_insert = dataset.connect('mysql+pymysql://writer:miaoji1109@10.10.149.146/private_data?charset=utf8')

        # hotel = db_insert['hotel']
        # hotel_room = db_insert['hotel_room']
        # hotel_room_price = db_insert['hotel_room_price']
        #
        # # 通过传递值生成新增数据
        # hotel_key_list = ['uid', ' ', 'hotel_name_en', 'map_info', 'city', 'city_mid', 'country', 'service',
        #                   'is_breakfast_free', 'ptid', 'img_list', 'star', 'id']
        #
        # _count = 0
        # hotel_data_list = []
        # for each, need_insert_hotel in get_hotel_info():
        #     _count += 1
        #     if _count % 10 == 0:
        #         print(_count)
        #     hotel_info = {key: each[key] for key in hotel_key_list}
        #     hotel_room_info = {
        #         'roomId': each['roomId'],
        #         'hid': each['uid'],
        #         'sid': each['sid'],
        #         'occ': each['occ'],
        #         'type': each['room_type'],
        #         'bed': each['bed_type'],
        #         'breakfast': '1',
        #         'ptid': each['ptid'],
        #     }
        #
        #     hotel_room_price_info_list = [{
        #         'roomId': each['roomId'],
        #         'date': date,
        #         'price': each['price'],
        #         'ccy': each['ccy'],
        #     } for date in Common.DateRange.dates_until('20171231')]
        #
        #     # print(hotel_info)
        #     # print(hotel_room_info)
        #     # print(hotel_room_price_info)
        #     if need_insert_hotel:
        #         hotel_data_list.append(hotel_info)
        #         # hotel.insert(hotel_info)
        #     hotel_room.insert(hotel_room_info)
        #     hotel_room_price.insert_many(hotel_room_price_info_list)
        # hotel.insert_many(hotel_data_list)
