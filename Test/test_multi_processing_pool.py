#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/18 下午12:28
# @Author  : Hou Rong
# @Site    : 
# @File    : test_multi_processing_pool.py
# @Software: PyCharm
from multiprocessing import Pool


def f(x):
    return x * x


if __name__ == '__main__':
    p = Pool(5)
    res = p.apply_async(f, [1, 2, 3])
    p.join()
    print(res.get())
