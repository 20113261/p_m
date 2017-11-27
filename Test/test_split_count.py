#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午6:40
# @Author  : Hou Rong
# @Site    : 
# @File    : test_split_count.py
# @Software: PyCharm


def split_word(w_list, split_len=1):
    for __i in range(len(w_list) - split_len + 1):
        yield ''.join(w_list[__i:__i + split_len])


def word_combination(w_list):
    for __i in range(1, min(3, len(w_list))):
        yield from split_word(w_list=w_list, split_len=__i)


def word_combination_without_prefix(w_list):
    if len(w_list) >= 1:
        yield ''.join(w_list[:1])
        if len(w_list) >= 2:
            yield ''.join(w_list[:2])
            if len(w_list) >= 3:
                yield ''.join(w_list[:3])


if __name__ == '__main__':
    # case = '和平国旅欧洲处'
    # print(list(full_split(case)))
    # case = '北京妙计科技有限公司'
    # word_list = list(jieba.cut(case))
    # print(word_list)
    word_list = ['北京', '妙计', '科技', '有限公司']
    print(list(word_combination(word_list)))
    print(list(word_combination_without_prefix(word_list)))
