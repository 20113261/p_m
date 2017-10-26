#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/26 下午12:39
# @Author  : Hou Rong
# @Site    : 
# @File    : get_all_pic.py
# @Software: PyCharm
import os
import random

suffix_set = {'png', 'jpg', 'jpeg'}

f_path = set()


def detect_all_pic_path(path):
    for each_path, folder, files in os.walk(path):
        if '副本' not in each_path and '待处理' not in each_path:
            for each_file in files:
                suffix = each_file.split('.')[-1]
                if suffix.lower() in suffix_set:
                    try:
                        cid, fid_suffix = each_file.split('_')
                        int(cid)
                        f_path.add(os.path.join(each_path, each_file))
                    except Exception:
                        pass


def main():
    path = '/Volumes/国家及城市图片/02_已处理图片（技术使用）/2.0国家城市图－已处理/1120紧急新增城市图片'
    detect_all_pic_path(path)
    for i in range(10):
        print(random.choice(list(f_path)))


if __name__ == '__main__':
    main()
