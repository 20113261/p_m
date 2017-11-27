#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午8:08
# @Author  : Hou Rong
# @Site    : 
# @File    : test_inherit.py
# @Software: PyCharm


class TestExc(Exception):
    def __init__(self):
        self.type = self.__class__.__name__


class A(TestExc):
    def __init__(self):
        super(A, self).__init__()


if __name__ == '__main__':
    a = A()
    print(a.type)
