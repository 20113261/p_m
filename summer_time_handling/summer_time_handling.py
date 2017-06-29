#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/29 下午2:25
# @Author  : Hou Rong
# @Site    : 
# @File    : summer_time_handling.py
# @Software: PyCharm
import dataset
import logging.handlers
from collections import defaultdict

# 因为本身时间不准而去除的城市 id
removed_city_id = ['12524', '12525', '12526', '12527', '12528', '12529', '12530', '12531', '12532', '12533', '12534',
                   '12535', '12536', '12537', '12538', '12889', '12890', '13497', '13498', '20692', '20693', '20694',
                   '20695', '20696', '20697', '20698', '20699', '20700', '20701', '20702', '20774', '20775', '20776',
                   '20777', '20778', '20779', '30228', '30229', '30234', '30235', '30238', '30240', '30241', '30243',
                   '30245', '30248', '30249', '30250', '30251', '30252', '30253', '30254', '30256', '30257', '30258',
                   '30262', '30263', '30264', '30265', '30267', '30268', '30270', '30271', '30272', '30274', '30275',
                   '30277', '30278', '30279', '30280', '30285', '30286', '30287', '30288', '30291', '30296', '30297',
                   '30298', '30301', '30304', '30306', '30308', '30309', '30310', '30311', '30314', '30316', '30317',
                   '30318', '30319', '30322', '40351', '40352', '40353', '40354', '40355', '40356', '40357', '40358',
                   '40359', '40360', '40361', '40362', '40363', '40364', '40365', '40401', '40402', '40403', '50804',
                   '51075', '51076', '51077', '60073', '60074', '60075', '60076', '60077']


def legal(string: str):
    return string.lower() not in ('null', '', 'summer_start', 'summer_end') and 'T' in string and not string.endswith(
        '1')


def modify(string: str):
    if len(string) == 16 and string.endswith('0'):
        string = string + ':00'
    return string.strip()


if __name__ == '__main__':
    DEBUG = False
    UPDATE_KEYS = ['country_id', 'time_zone', 'summer_zone']
    handler = logging.handlers.WatchedFileHandler(
        '/tmp/summer_time_modify_log',
        mode='w'
    )
    stream_handler = logging.StreamHandler()

    logger = logging.getLogger("SummerTimeHandler")
    logger.setLevel(level=logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)

    db = dataset.connect("mysql+pymysql://hourong:hourong@10.10.180.145/base_data?charset=utf8")
    table = db['city']
    right_start_summer_time = defaultdict(set)
    right_end_summer_time = defaultdict(set)
    right_summer_start_next_year = defaultdict(set)
    right_summer_end_next_year = defaultdict(set)

    count = 0
    for line in table.all():
        city_id = str(line['id'])
        country_id = int(line['country_id'])
        time_zone = float(line['time_zone'])
        summer_zone = float(line['summer_zone'])
        if city_id in removed_city_id:
            continue

        if summer_zone == -100:
            continue

        if summer_zone == 0 and time_zone not in (1, -1):
            continue
        'summer_start、summer_end、summer_start_next_year、summer_end_next_year'
        if legal(line['summer_start']):
            right_start_summer_time[(country_id, time_zone, summer_zone)].add(modify(line['summer_start']))

        if legal(line['summer_end']):
            right_end_summer_time[(country_id, time_zone, summer_zone)].add(modify(line['summer_end']))

        if legal(line['summer_start_next_year']):
            right_summer_start_next_year[(country_id, time_zone, summer_zone)].add(
                modify(line['summer_start_next_year']))

        if legal(line['summer_end_next_year']):
            right_summer_end_next_year[(country_id, time_zone, summer_zone)].add(modify(line['summer_end_next_year']))

    update_data = []
    for k, v in right_start_summer_time.items():
        if len(v) > 1:
            print('summer_start_err', k, v)
        else:
            logger.info('#' * 100)
            d = {
                'country_id': k[0],
                'time_zone': k[1],
                'summer_zone': k[2],
                'summer_start': v.pop()
            }
            for i in sorted(d.items(), key=lambda x: x[0]):
                logger.info('{0} => {1}'.format(*i))

            if not DEBUG:
                logger.info("update: {0}".format(table.update(d, keys=UPDATE_KEYS)))

    for k, v in right_end_summer_time.items():
        if len(v) > 1:
            print('summer_end_err', k, v)
        else:
            logger.info('#' * 100)
            d = {
                'country_id': k[0],
                'time_zone': k[1],
                'summer_zone': k[2],
                'summer_end': v.pop()
            }
            for i in sorted(d.items(), key=lambda x: x[0]):
                logger.info('{0} => {1}'.format(*i))
            if not DEBUG:
                logger.info("update: {0}".format(table.update(d, keys=UPDATE_KEYS)))

    for k, v in right_summer_start_next_year.items():
        if len(v) > 1:
            print('summer_start_next_year_err', k, v)
        else:
            logger.info('#' * 100)
            d = {
                'country_id': k[0],
                'time_zone': k[1],
                'summer_zone': k[2],
                'summer_start_next_year': v.pop()
            }
            for i in sorted(d.items(), key=lambda x: x[0]):
                logger.info('{0} => {1}'.format(*i))
            if not DEBUG:
                logger.info("update: {0}".format(table.update(d, keys=UPDATE_KEYS)))

    for k, v in right_summer_end_next_year.items():
        if len(v) > 1:
            print('summer_end_next_year_err', k, v)
        else:
            logger.info('#' * 100)
            d = {
                'country_id': k[0],
                'time_zone': k[1],
                'summer_zone': k[2],
                'summer_end_next_year': v.pop()
            }
            for i in sorted(d.items(), key=lambda x: x[0]):
                logger.info('{0} => {1}'.format(*i))
            if not DEBUG:
                logger.info("update: {0}".format(table.update(d, keys=UPDATE_KEYS)))

    logger.info('#' * 100)
