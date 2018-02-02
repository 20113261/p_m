#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
#新添城市路径
city_path = "/data/city/测试新增城市.xlsx"

#修改图片名,及更新图片路径
picture_path = "/Users/miojilx/Desktop/1206新增城市图"

#更新airport表 文件路径
airport_path = ""

#中间过程生成文件所在路径
base_path = '/search/service/nginx/html/MioaPyApi/store/opcity/'

#数据库配置
OpCity_config = {
        'host': '10.10.144.181',
        'user': 'writer',
        'password': 'miaoji1109',
        'port': 3306,
        'db': 'OpCity',
        'charset': 'utf8'
    }
config = {
        'host': '10.10.230.206',
        'user': 'mioji_admin',
        'password': 'mioji1109',
        'db': '',
        'charset': 'utf8'
}

ota_config = {
        'host': '10.10.230.206',
        'user': 'mioji_admin',
        'password': 'mioji1109',
        'db': 'source_info',
        'charset': 'utf8'
}
test_config = {
        'host': '10.10.69.170',
        'user': 'reader',
        'password': 'miaoji1109',
        'db': 'base_data',
        'charset': 'utf8'
}

#zip 文件所在路径

zip_path = '/Users/miojilx/Desktop/new_city.zip'


if __name__ == "__main__":
        pass


