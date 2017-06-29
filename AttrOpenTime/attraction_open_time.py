#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/26 下午3:37
# @Author  : Hou Rong
# @Site    : 
# @File    : attraction_open_time.py
# @Software: PyCharm
import logging.handlers
import pymysql
import re

sql_dict = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


class LevelFilter(logging.Filter):
    def __init__(self, pass_level, reject=False):
        super().__init__()
        self.pass_level = pass_level
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return record.levelno not in self.pass_level
        else:
            return record.levelno in self.pass_level


if __name__ == '__main__':
    handler = logging.handlers.WatchedFileHandler(
        '/tmp/open_time_translation_log',
        mode='w'
    )
    error_id_set = set()
    not_parse_count = 0
    unknown_month_count = 0
    unknown_week_count = 0
    error_official_count = 0
    error_year_count = 0
    unknown_length_count = 0
    stream_handler = logging.StreamHandler()
    # stream_filter = LevelFilter([logging.INFO, ])
    # stream_handler.addFilter(LevelFilter)

    logger = logging.getLogger("OpenTimeHandler")
    logger.setLevel(level=logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)

    conn = pymysql.connect(**sql_dict)
    cursor = conn.cursor()
    cursor.execute('''SELECT id,open
    FROM chat_attraction;''')
    for line in cursor.fetchall():
        for each in line[1].split('|'):
            res = re.findall('<([\s\S]+?)>', each)
            if len(res) == 0:
                logger.warning('[无法解析][each: {}][line: {}]'.format(each, line))
                not_parse_count += 1
                error_id_set.add(line[0])
                continue

            elif len(res) == 4:
                month_list = []
                week_list = []
                time_list = []
                official_key = None
                if '月' in res[0]:
                    month_list.append(res[0])
                elif res[0] == '*':
                    month_list.append('全年')
                else:
                    error_id_set.add(line[0])
                    unknown_month_count += 1
                    logger.warning('[未知月日字段][each: {}][line: {}]'.format(each, line))

                if '周' in res[1]:
                    week_list.append(res[1])
                elif res[1] == '*':
                    week_list.append('全周')
                else:
                    error_id_set.add(line[0])
                    unknown_week_count += 1
                    logger.warning('[未知周字段][each: {}][line: {}]'.format(each, line))

                time_list.extend(res[2].split('|'))

                if 'SURE' == res[3]:
                    official_key = '官方'
                elif 'NOT_SURE' == res[3]:
                    official_key = '非官方'
                else:
                    error_id_set.add(line[0])
                    error_official_count += 1
                    logger.warning('[官方字段错误][each: {}][line: {}]'.format(each, line))

            elif len(res) == 1:
                if res[0] != '全年':
                    error_id_set.add(line[0])
                    error_year_count += 1
                    logging.warning('[未定义的非全年字样][each: {}][line: {}]'.format(each, line))
            else:
                error_id_set.add(line[0])
                unknown_length_count += 1
                logger.warning('[未定义的解析长度][length: {}][each: {}][line: {}]'.format(len(res), each, line))

    '''
        not_parse_count = 0
    unknown_month_count = 0
    unknown_week_count = 0
    error_official_count = 0
    error_year_count = 0
    unknown_length_count = 0
    '''
    logger.debug('无法解析 {} 条'.format(not_parse_count))
    logger.debug('未知月日字段 {} 条'.format(unknown_month_count))
    logger.debug('未知周字段 {} 条'.format(unknown_week_count))
    logger.debug('官方字段错误 {} 条'.format(error_official_count))
    logger.debug('未定义的非全年字样 {} 条'.format(error_year_count))
    logger.debug('未定义的解析长度 {} 条'.format(unknown_length_count))

    # logger.error(str(error_id_set))
