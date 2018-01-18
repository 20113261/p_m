#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/18 上午8:58
# @Author  : Hou Rong
# @Site    : 
# @File    : prepare_google_drive_task.py
# @Software: PyCharm
import os

if __name__ == '__main__':
    # ---- Variables ----

    # file_path = '/root/data/task/traffic_total/url/traffic_poi'
    # file_path = '/root/data/task/0428'
    # dsc_file_name = '/root/data/task/traffic_poi'
    # dsc_file_name = '/root/data/task/google_task_0428'

    file_path = '/search/hourong/task/1227/url/'
    dsc_file_name = '/search/hourong/task/target_url_1227'
    max_line_count = 500000

    # -------------------
    __num_count = 0
    table_index = 900
    table_name = 'new_crawled_html_%03d' % table_index
    line_count = 0
    dsc_file = open(dsc_file_name, 'w')
    for file_name in os.listdir(file_path):
        file_path_name = os.path.join(file_path, file_name)
        for line in open(file_path_name):
            __num_count += 1
            if __num_count % 10000 == 0:
                print(__num_count)
            line_count += 1
            data = (line.strip(), file_name, table_name)
            dsc_file.write('###'.join(data) + '\n')
            if line_count == max_line_count:
                line_count = 0
                table_index += 1
                table_name = 'new_crawled_html_%02d' % table_index
    print(__num_count)
