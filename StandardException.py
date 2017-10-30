#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/30 下午12:59
# @Author  : Hou Rong
# @Site    : 
# @File    : StandardException.py
# @Software: PyCharm


class PoiTypeError(TypeError):
    def __init__(self, poi_type):
        self.poi_type = poi_type

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "Unknown Type: {}".format(self.poi_type)


if __name__ == '__main__':
    unknown_type_error = PoiTypeError("attr")
    raise unknown_type_error
