#!/usr/bin/env python
# -*- coding:utf-8 -*-

from city.add_city import read_file
from city.share_airport import update_share_airport,insert_airport
from city.update_city_pic import update_city_pic
from city.city_map_ciytName import revise_pictureNmae
from city.config import city_path
from city.config import picture_path
from city.config import airport_path
import pymysql
pymysql.install_as_MySQLdb()
import pandas
config = {
    'host': '10.10.230.206',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'tmp',
    'charset': 'utf8'
}
def database_template():
    conn = pymysql.connect(**config)
    data_temp = pandas.read_sql('select * from airport limit 10;',con=conn)
    data_temp.to_csv('data_temp.csv',index=False)

def start_task():

    if city_path:
        read_file(city_path)

    if picture_path:
        revise_pictureNmae(picture_path)
        update_city_pic(picture_path)

    if airport_path:
        insert_airport(airport_path)
        update_share_airport()




if __name__ == "__main__":
    start_task()