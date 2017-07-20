#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/14 上午11:33
# @Author  : Hou Rong
# @Site    : 
# @File    : GetMd5.py
# @Software: PyCharm
import hashlib


def encode_md5(string):
    return hashlib.md5(string.encode()).hexdigest()


if __name__ == '__main__':
    print(encode_md5('abc'))
