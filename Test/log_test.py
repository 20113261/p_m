#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/16 下午12:05
# @Author  : Hou Rong
# @Site    : 
# @File    : log_test.py
# @Software: PyCharm
from my_logger import get_logger

logger = get_logger('test_logger')
logger.debug("[just a test]")
