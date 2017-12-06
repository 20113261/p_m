#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午8:00
# @Author  : Hou Rong
# @Site    :
# @File    : test_memory.py
# @Software: PyCharm
from memory_profiler import profile


@profile
def my_func():
    a = [1] * (2 * 10 ** 7)
    b = [2] * (2 * 10 ** 7)

    return None


@profile
def my_func_2():
    a = [1] * (2 * 10 ** 7)
    b = [2] * (2 * 10 ** 7)

    del b
    del a

    return None


@profile
def print_func():
    print("Hello World")


if __name__ == '__main__':
    dd = [1] * (2 * 10 ** 7)
    my_func()
    cc = [1] * (2 * 10 ** 7)
    my_func_2()
    ee = [1] * (2 * 10 ** 7)
    print_func()
