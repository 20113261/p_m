#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import zipfile
import re
import os
from city.add_city import read_file
from city.city_map_ciytName import revise_pictureName
from city.field_check import city_field_check,check_repeat_city,city_must_write_field,airport_field_check,check_repeat_airport,airport_must_write_field,new_airport_insert
from city.field_check import check_new_city_id
from city.share_airport import update_share_airport
from city.update_city_pic import update_city_pic
from city.db_insert import shareAirport_insert
from city.config import base_path,config,zip_path

class CityControlFlow():

    def __init__(self,config):
        self.config = config
        self.type = None

    def _execute(self,city_path=None,airport_path=None,picture_path=None):

        if city_path:
            is_must = city_must_write_field(city_path=city_path)
            if not is_must:
                return False

            is_repeat = check_repeat_city(city_path=city_path,config=self.config)
            if not is_repeat:
                return False
            is_field = city_field_check(city_path=city_path)
            if not is_field:
                return False
            read_file(xlsx_path=city_path,config=self.config)
            is_exist_id = check_new_city_id(config=self.config)
            if not is_exist_id:
                return False
            if picture_path:
                revise_pictureName(path=picture_path,config=self.config)
                update_city_pic(path=picture_path,config=self.config)
            update_share_airport()
            shareAirport_insert(config=self.config)

        if airport_path:
            is_must = airport_must_write_field(airport_path,config=self.config)
            if not is_must:
                return False
            is_repeat = check_repeat_airport(airport_path,config=self.config)
            if not is_repeat:
                return False
            is_field = airport_field_check(airport_path,config=self.config)
            if not is_field:
                return False
            new_airport_insert(config=self.config)

    def task_start(self):
        zip = zipfile.ZipFile(zip_path)
        file_name = zip.filename.split('.')[0].split('/')[-1]
        if base_path.endswith('/'):
            file_path = ''.join([base_path,file_name])
        else:
            file_path = '/'.join([base_path,file_name])
        file_list = os.listdir(file_path)

        for child_file in file_list:
            print(child_file)
            path = '/'.join([file_path,child_file])
            if os.path.isdir(path):
                picture_path = path
            elif '城市' in child_file:
                city_path = path
            elif '机场' in child_file:
                airport_path = path
        self._execute(city_path=city_path,airport_path=airport_path,picture_path=picture_path)


if __name__ == "__main__":

    param = sys.argv[1]
    config['db'] = ''.join(['add_city_',str(param)])
    # print('param:',param)

    print(config)
    city = CityControlFlow(config)
    city.task_start()
