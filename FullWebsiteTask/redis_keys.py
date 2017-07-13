#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/12 上午10:03
# @Author  : Hou Rong
# @Site    : 
# @File    : redis_keys.py
# @Software: PyCharm
import redis

r = redis.Redis(host='10.10.213.148', db=15)

if __name__ == '__main__':
    _count = 0
    res = []
    for key in r.keys():
        num = r.scard(key)
        _count += num
        res.append((key, num))

    for k, n in sorted(res, key=lambda x: x[1]):
        print(k, n)

    print(_count)
