#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/23 下午2:06
# @Author  : Hou Rong
# @Site    : 
# @File    : CommonBaseDataInsert.py
# @Software: PyCharm
import dataset
import abc
from qyoa_base_data import settings


class BaseDataInsert(object):
    def __init__(self, keys, key_map, table_name, unique_key, need_update_city_id):
        self.keys = keys
        self.key_map = key_map
        self.src_table_name, self.dst_table_name = table_name
        self.unique_key = unique_key
        self.private_db = dataset.connect(settings.PRIVATE_DB_STR)
        self.target_db = dataset.connect(settings.TARGET_DB_STR)
        self.id_map_db = dataset.connect(settings.ID_MAP_DB_STR)
        self.data_id_set = set()
        self.id_map = self.get_id_map()
        self.need_update_city_id = need_update_city_id

    def get_data(self):
        for line in self.private_db.query(
                'SELECT * from {0} WHERE ptid = "qyoa" AND disable = 0'.format(self.src_table_name)):
            data = {}
            for k in self.keys:
                # 去除空行
                if line[k]:
                    if line[k] != '' and line[k] != 'NULL':
                        data[(self.key_map.get(k, None) or k)] = line[k]

            # 从 refer 中获取 id
            if line['refer'] is not None:
                if line['refer'] != '':
                    data['id'] = line['refer']

            # 从 id_map 中获取 id
            if 'id' not in data:
                id_map_miaoji_id = self.id_map.get(line['id'], None)
                if id_map_miaoji_id:
                    data['id'] = id_map_miaoji_id

            # 新生成相关 id
            if 'id' not in data:
                data['id'] = self.get_new_id()

            # 景点，购物，餐厅更新 city_id
            if self.need_update_city_id:
                data['city_id'] = self.id_map.get(data['city_id'], None) or data['city_id']

            # 保存本次所有 id
            self.data_id_set.add(data['id'])

            # 存储 id 对应关系
            self.insert_id_map(line['id'], data['id'])

            # 修改 景点、购物、餐厅 表 status
            if self.need_update_city_id:
                data['status_test'] = "Open"
                data['status_online'] = "Close"
                data['dept_status_online'] = "Close"
                data['dept_status_test'] = "Close"
            # 增加 city 表 status
            else:
                data['status_test'] = "Open"

            yield data

    def insert(self, data):
        self.target_db[self.dst_table_name].upsert(data, keys=self.unique_key)

    def insert_id_map(self, private_db_id, miaoji_id):
        self.id_map_db['QyoaIdMap'].upsert({
            'private_db_id': private_db_id,
            'miaoji_id': miaoji_id
        }, keys=['private_db_id', 'miaoji_id'])

    def get_id_map(self):
        _dict = {}
        for line in self.id_map_db['QyoaIdMap'].all():
            _dict[line['private_db_id']] = line['miaoji_id']
        return _dict

    def run(self):
        for data in self.get_data():
            self.insert(data)
        print(self.data_id_set)

    @abc.abstractclassmethod
    def get_new_id(self):
        pass


class CityBaseDataInsert(BaseDataInsert):
    def __init__(self):
        super().__init__(settings.CITY_KEYS, settings.CITY_KEY_MAP, ('city', 'city'), settings.CITY_UNIQUE_KEY, False)

    def get_new_id(self):
        return int(list(self.target_db.query('''SELECT max(id) + 1 AS new_id FROM city;'''))[0]['new_id'])


class AttrBaseDataInsert(BaseDataInsert):
    def __init__(self):
        super().__init__(settings.ATTR_KEYS, settings.ATTR_KEY_MAP, ('attraction', 'chat_attraction'),
                         settings.ATTR_UNIQUE_KEY, True)

    def get_new_id(self):
        return list(
            self.target_db.query(
                '''SELECT concat('v', substr(max(id), 2) + 1) AS new_id FROM chat_attraction;'''
            )
        )[0]['new_id'].decode()


class RestBaseDataInsert(BaseDataInsert):
    def __init__(self):
        super().__init__(settings.REST_KEYS, settings.REST_KEY_MAP, ('restaurant', 'chat_restaurant'),
                         settings.ATTR_UNIQUE_KEY, True)

    def get_new_id(self):
        pass


class ShopBaseDataInsert(BaseDataInsert):
    def __init__(self):
        super().__init__(settings.SHOP_KEYS, settings.SHOP_KEY_MAP, ('shopping', 'chat_shopping'),
                         settings.SHOP_UNIQUE_KEY, True)

    def get_new_id(self):
        pass


if __name__ == '__main__':
    city_base_data_insert = CityBaseDataInsert()
    city_base_data_insert.run()

    attr_base_data_insert = AttrBaseDataInsert()
    attr_base_data_insert.run()
