#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/29 下午6:04
# @Author  : Hou Rong
# @Site    : 
# @File    : MergeSimilarData.py
# @Software: PyCharm
import dataset
from qyoa_base_data import settings


class MergeData(object):
    def __init__(self):
        self.table_name = 'chat_attraction'
        self.unique_key = 'id'
        self.merge_dict = {'v639970': 'v513396', 'v639969': 'v517786'}
        self.target_db = dataset.connect(settings.TARGET_DB_STR)

    def get_data(self):
        target_ids = []
        target_ids.extend(self.merge_dict.keys())
        target_ids.extend(self.merge_dict.values())
        return {
            line['id']: line for line in
            self.target_db.query('''SELECT * FROM {0} WHERE id IN ({1})'''.format(
                self.table_name,
                ','.join(
                    map(lambda x: '"{0}"'.format(x), target_ids)
                )
            )
            )
        }

    def merge(self):
        _data = self.get_data()
        for key, value in self.merge_dict.items():
            src_data = _data[key]
            dst_data = _data[value]

            for k in src_data.keys():
                # 去除空行
                if src_data[k]:
                    if src_data[k] != '' and src_data[k] != 'NULL' and src_data[k] != '-1':
                        dst_data[k] = src_data[k]

            yield dst_data

    def insert(self, data):
        return self.target_db[self.table_name].upsert(data, keys=self.unique_key)

    def run(self):
        for d in self.merge():
            print('#' * 100)
            print(d['id'])
            print(self.insert(d))


if __name__ == '__main__':
    MD = MergeData()
    MD.run()
