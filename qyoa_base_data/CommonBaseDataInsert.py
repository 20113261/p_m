#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/23 下午2:06
# @Author  : Hou Rong
# @Site    : 
# @File    : CommonBaseDataInsert.py
# @Software: PyCharm
import dataset
import abc
import json
import copy
from qyoa_base_data import settings
from toolbox.Common import is_legal


class BaseDataInsert(object):
    def __init__(self, keys, key_map, table_name, unique_key, need_update_city_id, need_add_id_map=True,
                 need_new_data=True, need_update_status=True):
        self.keys = keys
        self.key_map = key_map
        self.src_table_name, self.dst_table_name = table_name
        self.unique_key = unique_key
        self.private_db = dataset.connect(settings.PRIVATE_DB_STR)
        self.target_db = dataset.connect(settings.TARGET_DB_STR)
        self.id_map_db = dataset.connect(settings.ID_MAP_DB_STR)
        self.data_id_set = set()
        self.errors = list()
        self.id_map = self.get_id_map()
        self.need_update_city_id = need_update_city_id
        self.id_key = unique_key[0]
        self.need_add_id_map = need_add_id_map
        self.need_new_data = need_new_data
        self.need_update_status = need_update_status
        self.total = 0
        self.update = 0
        self.default_val = {}
        self.skip_keys = settings.ATTR_SKIP_KEYS

    def update_default_val(self, d: dict):
        self.default_val.update(d)

    def get_data(self):
        for line in self.private_db.query(
                '''SELECT * from {0} WHERE ptid = "qyoa" AND disable = 0 AND city_id=50016;'''.format(
                    self.src_table_name)):
            data = copy.deepcopy(self.default_val)
            for k in self.keys:
                # 去除不使用的值
                if k in self.skip_keys:
                    continue
                # 去除空行
                if is_legal(line[k]):
                    data[(self.key_map.get(k, None) or k)] = line[k]

            # 从 refer 中获取 id
            if line['refer'] is not None:
                if line['refer'] != '':
                    data[self.id_key] = line['refer']

            if not self.need_new_data:
                if self.id_key not in data:
                    self.errors.append(line)
                    continue
                if not data[self.id_key]:
                    self.errors.append(line)
                    continue

            # 如果需要增加 id map 的关系，则使用如此方法获取 id
            if self.need_add_id_map:
                # 从 id_map 中获取 id
                if self.id_key not in data:
                    id_map_miaoji_id = self.id_map.get(line[self.id_key], None)
                    if id_map_miaoji_id:
                        data[self.id_key] = id_map_miaoji_id

                # 新生成相关 id
                if self.id_key not in data:
                    data[self.id_key] = self.get_new_id()

            # 景点，购物，餐厅更新 city_id
            if self.need_update_city_id:
                data['city_id'] = self.id_map.get(data['city_id'], None) or data['city_id']

            # 保存本次所有 id
            self.data_id_set.add(data[self.id_key])

            # 如果需要增加 id 对应关系
            if self.need_add_id_map:
                # 存储 id 对应关系
                self.insert_id_map(line[self.id_key], data[self.id_key])

            if self.need_update_status:
                # 修改 景点、购物、餐厅 表 status
                if self.need_update_city_id:
                    data['status_test'] = "Open"
                    data['status_online'] = "Open"
                    data['dept_status_online'] = "Open"
                    data['dept_status_test'] = "Open"
                # 增加 city 表 status
                else:
                    data['status_test'] = "Open"
                    data['status_online'] = "Open"

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
                         settings.ATTR_UNIQUE_KEY, need_update_city_id=False, need_add_id_map=True,
                         need_new_data=True,
                         need_update_status=True)
        self.update_default_val(settings.ATTR_DEFAULT_VALUE)

    def get_new_id(self):
        return list(
            self.target_db.query(
                '''SELECT concat('v', substr(max(id), 2) + 1) AS new_id FROM chat_attraction;'''
            )
        )[0]['new_id']


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


class HotelBaseDataInsert(BaseDataInsert):
    def __init__(self):
        super().__init__(settings.HOTEL_KEYS, settings.HOTEL_KEY_MAP, ('hotel', 'hotel'), settings.HOTEL_UNIQUE_KEY,
                         False, need_add_id_map=False, need_new_data=False, need_update_status=False)
        import pymysql
        self.conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8',
                                    db='base_data')

    def get_new_id(self):
        pass

    def insert(self, data: dict):
        self.total += 1
        with self.conn.cursor() as cursor:
            sql_temp = []
            sql_data = []
            data['official'] = 1
            for k, v in data.items():
                if k != 'rec_priority':
                    sql_temp.append('`{0}`=%s'.format(k))
                    sql_data.append(v)
                elif v in (10, '10'):
                    sql_temp.append('`commercial_mark`=%s')
                    sql_data.append(json.dumps({'type': 0, 'title': '洛杉矶旅游局会员酒店'}))

            sql = 'UPDATE hotel_online_test SET {0} WHERE uid=%s'.format(','.join(sql_temp))
            sql_data.append(data['uid'])
            self.update += cursor.execute(sql, sql_data)
        print('Total: {0}, Update: {1}'.format(self.total, self.update))


if __name__ == '__main__':
    # city_base_data_insert = CityBaseDataInsert()
    # city_base_data_insert.run()
    #
    attr_base_data_insert = AttrBaseDataInsert()
    attr_base_data_insert.run()

    # hotel_base_data_insert = HotelBaseDataInsert()
    # hotel_base_data_insert.run()
