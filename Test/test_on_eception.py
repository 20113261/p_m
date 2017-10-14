#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/14 下午3:39
# @Author  : Hou Rong
# @Site    : 
# @File    : test_on_eception.py
# @Software: PyCharm
import functools
import traceback
import inspect
from Test.test_cases import main as main_cases, main_exc as main_exc_cases


def on_exc_send_email(func):
    @functools.wraps(func)
    def wrapper():
        try:
            func_file = inspect.getfile(func)
        except Exception:
            try:
                func_file = inspect.getabsfile(func)
            except Exception:
                func_file = 'may be local func: {}'.format(func.__name__)
        try:
            func()
            print("OK", func_file, func.__name__)
        except Exception:
            print(traceback.format_exc())

    return wrapper


if __name__ == '__main__':
    on_exc_send_email(func=main_cases)()
