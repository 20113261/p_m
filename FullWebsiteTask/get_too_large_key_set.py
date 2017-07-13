#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/12 上午10:15
# @Author  : Hou Rong
# @Site    : 
# @File    : get_too_large_key_set.py
# @Software: PyCharm
import redis

r = redis.Redis(host='10.10.213.148', db=14)

if __name__ == '__main__':
    _count = 0
    for i in r.smembers('http://www.ccc.govt.nz/rec-and-sport/cycling-tracks/mountain-biking/rapaki-track'):
        print(i)
        _count += 1
        if _count == 100:
            break
