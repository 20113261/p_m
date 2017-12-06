#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午8:19
# @Author  : Hou Rong
# @Site    : 
# @File    : test_func_code.py
# @Software: PyCharm
import dis


def my_func():
    a = [1] * (2 * 10 ** 7)
    b = [2] * (2 * 10 ** 7)

    return None


def my_func_2():
    a = [1] * (2 * 10 ** 7)
    b = [2] * (2 * 10 ** 7)

    del b
    del a

    return None


def print_func():
    print("Hello World")


def while_():
    while True:
        pass


'''
 12           0 LOAD_CONST               1 (1)
              3 BUILD_LIST               1
              6 LOAD_CONST               6 (20000000)
              9 BINARY_MULTIPLY
             10 STORE_FAST               0 (a)

 13          13 LOAD_CONST               2 (2)
             16 BUILD_LIST               1
             19 LOAD_CONST               8 (20000000)
             22 BINARY_MULTIPLY
             23 STORE_FAST               1 (b)

 15          26 LOAD_CONST               0 (None)
             29 RETURN_VALUE



19           0 LOAD_CONST               1 (1)
              3 BUILD_LIST               1
              6 LOAD_CONST               6 (20000000)
              9 BINARY_MULTIPLY
             10 STORE_FAST               0 (a)

 20          13 LOAD_CONST               2 (2)
             16 BUILD_LIST               1
             19 LOAD_CONST               8 (20000000)
             22 BINARY_MULTIPLY
             23 STORE_FAST               1 (b)

 22          26 DELETE_FAST              1 (b)

 23          29 DELETE_FAST              0 (a)

 25          32 LOAD_CONST               0 (None)
             35 RETURN_VALUE


 33           0 SETUP_LOOP               4 (to 7)

 34     >>    3 JUMP_ABSOLUTE            3
              6 POP_BLOCK
        >>    7 LOAD_CONST               0 (None)
             10 RETURN_VALUE
'''


if __name__ == '__main__':
    # dis.dis(my_func)

    # dis.dis(my_func_2)

    dis.dis(while_)
