#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/1 下午11:46
# @Author  : Hou Rong
# @Site    : 
# @File    : MergeObj.py
# @Software: PyCharm


class Poi(object):
    def __init__(self, uid, union):
        self.uid = uid
        self.keys = set()
        self.union_keys = [union, ]

    def add_key(self, key, separator=None, legal_filter=is_legal):
        if separator:
            for each in key.split(separator):
                if legal_filter(each):
                    self.keys.add(each)
        else:
            if legal_filter(key):
                self.keys.add(key)

    def is_similar(self, key, separator=None):
        if not separator:
            if is_legal(key):
                if key in self.keys:
                    return True
        else:
            for each in key.split(separator):
                if is_legal(each):
                    if each in self.keys:
                        return True
        return False

    def add_union(self, union):
        self.union_keys.append(union)
