#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午5:57
# @Author  : Hou Rong
# @Site    : 
# @File    : test_split_word.py
# @Software: PyCharm


def split_word(word, split_len=1):
    for __i in range(len(word) - split_len + 1):
        res = word[__i:__i + split_len].strip()
        yield res


def full_split(word):
    for __i in range(1, min(word,)):
        yield from split_word(word=word, split_len=__i)


if __name__ == '__main__':
    case = '和平国旅欧洲处'
    print(list(full_split(case)))
