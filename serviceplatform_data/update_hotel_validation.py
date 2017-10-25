#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/24 上午10:22
# @Author  : Hou Rong
# @Site    : 
# @File    : update_hotel_validation.py
# @Software: PyCharm
import pymysql
from warnings import filterwarnings
from urllib.parse import urlparse, urljoin
from data_source import MysqlSource
from logger import get_logger
from service_platform_conn_pool import verify_info_pool

filterwarnings('ignore', category=pymysql.err.Warning)
logger = get_logger("update_hotel_validation")

test_db = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

online_db = {
    'host': '10.10.68.103',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

offset = 0


def default_api_task_key_and_content(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    content = '{}&{}&'.format(each_data["mid"], each_data["sid"])
    return workload_key, content, workload_source


def sid_only_key_and_content(each_data, double_key=False):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    if not double_key:
        content = '{}&'.format(each_data["sid"])
    else:
        content = '{}&&'.format(each_data["sid"])
    return workload_key, content, workload_source


def booking(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    # 1231656&hotel/us/victorian-alamo-square-two-bedroom-apartment&
    tmp_url = urlparse(each_data['hotel_url']).path[1:]
    content = '{}&{}&'.format(each_data["sid"], tmp_url.split('.zh-cn.html')[0])
    return workload_key, content, workload_source


def elong(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    # NULL&&294945&&圣彼得无限酒店&&NULL&&
    content = 'NULL&&{}&&{}&&NULL&&'.format(each_data["sid"], each_data["name"])
    return workload_key, content, workload_source


def expedia(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    # http://www.expedia.com.hk/cn/Sapporo-Hotels-La'gent-Stay-Sapporo-Oodori-Hokkaido.h15110395.Hotel-Information?&
    content = "https://www.expedia.com.hk/cn" + urlparse(each_data['hotel_url']).path + '?&'
    return workload_key, content, workload_source


def agoda(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    # http://www.expedia.com.hk/cn/Sapporo-Hotels-La'gent-Stay-Sapporo-Oodori-Hokkaido.h15110395.Hotel-Information?&
    content = urljoin("https://www.agoda.com", urlparse(each_data['hotel_url']).path) + '&'
    return workload_key, content, workload_source


def hotels(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    # 洛杉矶&&美国&&1439028&&163102&&1&&20171010
    content = 'NULL&&NULL&&NULL&&{}&&'.format(each_data["sid"])
    return workload_key, content, workload_source


def hilton(each_data):
    workload_source = each_data["source"] + "Hotel"
    workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
    content = 'NULL&{}&{}&'.format(each_data["mid"], each_data["sid"])
    return workload_key, content, workload_source


def insert_data(data, _count):
    replace_sql = '''INSERT IGNORE INTO workload_hotel_validation (workload_key, content, source, extra, status) 
    VALUES (%s, %s, %s, 0, 1);'''

    max_retry_times = 3
    while max_retry_times:
        max_retry_times -= 1
        try:
            conn = verify_info_pool.connection()
            cursor = conn.cursor()
            _insert_count = cursor.executemany(replace_sql, data)
            conn.commit()
            cursor.close()
            conn.close()
            logger.debug(
                "[insert data][now count: {}][insert data: {}][insert_ignore_count: {}]".format(_count, len(data),
                                                                                                _insert_count))
            break
        except Exception as exc:
            logger.exception(msg="[run sql error]", exc_info=exc)


def update_per_hotel_validation(start=0, env='test'):
    global offset
    _count = 0
    data = []

    if env == 'test':
        db_conf = test_db
    elif env == 'online':
        db_conf = online_db
    else:
        raise TypeError("Unknown Env: {}".format(env))

    sql = '''SELECT
      source,
      sid,
      uid,
      mid,
      name,
      name_en,
      hotel_url
    FROM hotel_unid WHERE source='ctrip' LIMIT {},999999999999;'''.format(start)
    for line in MysqlSource(db_conf, table_or_query=sql, size=10000, is_table=False, is_dict_cursor=True):
        source = line['source']
        try:
            if source in (
                    "ctripcn", "yundijie", "daolvApi", "dotwApi", "expediaApi", "gtaApi", "hotelbedsApi", "innstantApi",
                    "jacApi", "mikiApi", "touricoApi"):
                each_data = default_api_task_key_and_content(line)
                data.append(each_data)
            elif source in ("expedia", "cheaptickets", "orbitz", "ebookers", "travelocity"):
                # ep 系，使用 url 类型的
                each_data = expedia(line)
                data.append(each_data)
            elif source in ("hrs", "ctrip"):
                # 单纯 sid 的
                each_data = sid_only_key_and_content(line)
                data.append(each_data)
            elif source in ("marriott",):
                # 单纯 sid 的，两个 &&
                each_data = sid_only_key_and_content(line, double_key=True)
                data.append(each_data)
            elif source == "hilton":
                # hilton 专用，content 前面多了一个 NULL
                each_data = hilton(line)
                data.append(each_data)
            elif source == "booking":
                # booking 专用
                each_data = booking(line)
                data.append(each_data)
            elif source == "elong":
                # elong 专用
                each_data = elong(line)
                data.append(each_data)
            elif source == "hotels":
                # hotels 专用
                each_data = hotels(line)
                data.append(each_data)
            elif source == "agoda":
                # agoda 专用
                each_data = agoda(line)
                data.append(each_data)
            elif source in (
                    "accor", "hoteltravelEN", "hoteltravel", "venere", "venereEN", "agodaApi",
                    "amoma",
                    "haoqiaoApi", "hostelworld", "hotelclub", "ihg", "kempinski", "starwoodhotels", "tongchengApi"):
                # 不更新 workload validation
                # hoteltravel, venere 源被下掉了
                # 由于 ctrip 爬虫当前倒了，本次不更新 ctrip
                '''
                这些源当前不进行验证，不生成相关任务
                agodaApiHotel
                amomaHotel
                haoqiaoApiHotel
                hostelworldHotel
                hotelclubHotel
                ihgHotel
                kempinskiHotel
                starwoodhotelsHotel
                '''
                pass
            else:
                logger.warning("[Unknown Source: {}]".format(source))
        except Exception as exc:
            logger.exception(msg="[make workload key has exception][source: {}]".format(source), exc_info=exc)

        _count += 1
        offset += 1
        if len(data) == 1000:
            # replace into validation data
            insert_data(data, offset)
            data = []

    # replace into validation data
    insert_data(data, _count)


def update_per_env_hotel_validation(env):
    global offset
    max_retry_times = 10000
    while max_retry_times:
        max_retry_times -= 1
        try:
            update_per_hotel_validation(start=offset, env=env)
            break
        except Exception as exc:
            logger.exception(msg="[update hotel validation error]", exc_info=exc)


def update_hotel_validation():
    # global offset
    update_per_env_hotel_validation('test')
    # offset = 0
    # update_per_env_hotel_validation('online')


if __name__ == '__main__':
    update_hotel_validation()