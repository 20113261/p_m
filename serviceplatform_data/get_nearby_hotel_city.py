#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 下午2:41
# @Author  : Hou Rong
# @Site    : 
# @File    : get_nearby_hotel_city.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105', port=27017)
db = client['HotelData']
collections = client['HotelData']['detail_final_data']


def get_per_nearby_city(city_map_info):
    lon, lat = city_map_info.split(',')
    for each in collections.aggregate({"loc":
        {
            "$geoNear": {"$geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },
                "$maxDistance": 50000}
        },
    }):
        print(each)


if __name__ == '__main__':
    get_per_nearby_city("2.351492339147967,48.85746107178952")

    '''
    # Mongo 获取带经纬度距离的方法
     result = db.command({"geoNear": "detail_final_data",
                         "near": {"type": "Point", "coordinates": [2.351492339147967, 48.85746107178952]},
                         "spherical": True,
                         "maxDistance": 50000})
    _count = 0
    for each in result["results"]:
        print(each)

    print(_count)
    '''
