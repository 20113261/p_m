#!usr/bin/env python
# -*- coding:utf-8 -*-

import csv
import pymysql
pymysql.install_as_MySQLdb()
import sys
import os
config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'db': 'base_data',
    'charset': 'utf8'
}

conn = pymysql.connect(**config)

def get_cityName():
    cursor = conn.cursor()
    select_sql = "select id,name,name_en from city where id=%s"
    with open('city_id.csv','r+') as city:
        reader = csv.reader(city)
        next(reader)
        with open('map_cityName.csv','w+') as name:
            writer = csv.writer(name)
            writer.writerow(('city_id','name','name_en'))
            for row in reader:
                cursor.execute(select_sql,row)
                result = cursor.fetchone()
                writer.writerow(result)

def revise_pictureNmae(path):
    get_cityName()
    city_map = {}
    with open('map_cityName.csv','r+') as city:
        reader = csv.reader(city)
        next(reader)
        for row in reader:
            city_map[row[1]] = row[0]
            city_map[row[2]] = row[0]
    file_names = os.listdir(path)
    file_names.remove('.DS_Store')
    for file_name in file_names:
        real_name,extend_name = os.path.splitext(file_name)
        city_name = real_name.split('_')[0]
        if city_map.get(city_name):
            new_file_name = ''.join([city_map.get(city_name),'_',real_name.split('_')[1],extend_name])
            old_name = '/'.join(path,file_name)
            new_name = '/'.join(path,new_file_name)
            os.rename(old_name,new_name)

if __name__ == "__main__":
    revise_pictureNmae()
