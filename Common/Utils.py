#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/20 上午11:38
# @Author  : Hou Rong
# @Site    : 
# @File    : Utils.py
# @Software: PyCharm


def is_legal(s):
    if s:
        if isinstance(s, str):
            if s.strip():
                if s.lower() != 'null':
                    return True
        elif isinstance(s, int):
            if s > -1:
                return True

        elif isinstance(s, float):
            if s > -1.0:
                return True
    return False
