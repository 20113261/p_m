#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/12 上午11:06
# @Author  : Hou Rong
# @Site    : 
# @File    : CtripHotelDetailTask.py
# @Software: PyCharm
import pymongo
from collections import defaultdict

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['HotelList']['ctrip']

if __name__ == '__main__':
    # id_set = set()
    # _count = 0
    # for line in collections.find():
    #     _count += 1
    #     id_set.add(line['sid'])
    #     if _count % 10000 == 0:
    #         print(_count)
    #
    # f = open('/root/data/task/ctrip_sid', 'w')
    # for sid in id_set:
    #     f.write("{}\n".format(sid))
    #
    # print('total length', len(id_set))
    # f.close()

    sid_cid_set = set()
    for line in collections.find():
        sid_cid_set.add((line['parent_info']['city_id'], line['sid']))

    f = open('/root/data/task/ctrip_cid_sid', 'w')
    _count = 0
    for cid, sid in sid_cid_set:
        _count += 1
        if _count % 10000 == 0:
            print(_count)
        f.write("{}\t{}\n".format(cid, sid))

    print('total length', len(sid_cid_set))
    f.close()
