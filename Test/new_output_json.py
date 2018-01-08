#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/28 下午9:58
# @Author  : Hou Rong
# @Site    : 
# @File    : new_output_json.py
# @Software: PyCharm
import json
import os.path
import gzip
from data_source import MysqlSource
from logger import get_logger

logger = get_logger("mk_google_drive_data")

db_config = {
    'host': '10.10.231.105',
    'user': 'hourong',
    'password': 'hourong',
    'db': 'crawled_html',
    'charset': 'utf8'
}

if __name__ == '__main__':
    # ----- Variables ------

    need_compress = True
    # FOLDER_PATH = '/search/google_drive/google_drive_1227'
    FOLDER_PATH = '/tmp'
    # FOLDER_PATH = '/root/data/data_output/google_drive'
    # all_table_list = ['new_crawled_html_450','new_crawled_html_451','new_crawled_html_452']
    all_table_list = ['new_crawled_html_900', 'new_crawled_html_901', 'new_crawled_html_902', 'new_crawled_html_903',
                      'new_crawled_html_904', 'new_crawled_html_905', 'new_crawled_html_906', 'new_crawled_html_907',
                      'new_crawled_html_908', 'new_crawled_html_909']

    # ----------------------

    for table_name in all_table_list:
        # create path
        PATH = os.path.join(FOLDER_PATH, table_name)
        if not os.path.exists(PATH):
            os.mkdir(PATH)

        # write file
        file_num = 1
        writer = None
        _count = 0
        _total_count = 0

        sql = '''SELECT url,content FROM {0};'''.format(table_name)

        for url, content in MysqlSource(db_config, table_or_query=sql,
                                        size=10000, is_table=False,
                                        is_dict_cursor=False):
            if not writer:
                if need_compress:
                    f_name = '{0}_{1:03d}.json.gz'.format(table_name, file_num)
                    writer = gzip.open(os.path.join(PATH, f_name), 'wb')
                else:
                    f_name = '{0}_{1:03d}.json'.format(table_name, file_num)
                    writer = open(os.path.join(PATH, f_name), 'w')

            json_data = {'url': url, 'content': content}
            json_str = json.dumps(json_data)
            if need_compress:
                writer.write(json_str.encode() + b'\n')
            else:
                writer.write(json_str + '\n')

            _count += 1
            _total_count += 1
            if _count == 1000:
                _count = 0
                file_num += 1
                writer.close()
                writer = None
                logger.info(_total_count)
        logger.info(_total_count)
