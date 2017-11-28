#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午10:02
# @Author  : Hou Rong
# @Site    : 
# @File    : drop_task_db.py
# @Software: PyCharm
import pymongo

client = pymongo.MongoClient(host='10.10.231.105')
db = client['MongoTask']


def drop_collections():
    for name in filter(lambda x: x.endswith('_20171127a'), db.collection_names()):
        print(name)
        db.drop_collection(name_or_collection=name)


if __name__ == '__main__':
    drop_collections()
