#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys

from city.add_city import read_file
from city.city_map_ciytName import revise_pictureNmae
from city.field_check import city_field_check,check_repeat_city,city_must_write_field,airport_field_check,check_repeat_airport,airport_must_write_field,new_airport_insert
from city.field_check import check_new_city_id
from city.share_airport import update_share_airport
from city.update_city_pic import update_city_pic
from city.db_insert import shareAirport_insert
from city.config import picture_path
from city.config import airport_path
from city.config import city_path

class CityControlFlow():

    def __init__(self,config,add_type):
        self.config = config
        self.type = add_type

    def _execute(self):

        if self.type == 'city':
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
            revise_pictureNmae(path=picture_path,config=self.config)
            update_city_pic(path=picture_path,config=self.config)
            update_share_airport()
            shareAirport_insert(config=self.config)

        elif self.type == 'airport':
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
        self._execute()

if __name__ == "__main__":
    config = {
        'host': '10.10.230.206',
        'user': 'mioji_admin',
        'password': 'mioji1109',
        'port': 3306,
        'db': '',
        'charset': 'utf8'
    }
    param = sys.argv[1]
    add_type = sys.argv[2]
    config['db'] = ''.join(['add_city_',str(param)])
    print('param:',param)
    print('add_type:',add_type)
    print(config)
    city = CityControlFlow(config,add_type)
    city.task_start()