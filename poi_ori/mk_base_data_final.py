#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 上午9:11
# @Author  : Hou Rong
# @Site    : 
# @File    : mk_base_data_final.py
# @Software: PyCharm
import os
import datetime
from poi_ori.update_tag_id import update_tag_id
from service_platform_conn_pool import poi_ori_pool
from logger import get_logger

logger = get_logger("mk_base_data")


def mk_base_data_final(_poi_type):
    if _poi_type == 'attr':
        table_name = 'chat_attraction'
    elif _poi_type == 'rest':
        table_name = 'chat_restaurant'
    elif _poi_type == 'shop':
        table_name = 'chat_shopping'
    else:
        raise TypeError("Unknown Type: {}".format(_poi_type))
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    if _poi_type == 'attr':
        cursor.execute('''TRUNCATE base_data.{};'''.format(table_name))
        conn.commit()
        sql = '''REPLACE INTO base_data.{0}
  SELECT
    id,
    name,
    alias,
    name_en,
    name_en_alias,
    map_info,
    city_id,
    grade,
    ranking,
    '',
    address,
    introduction,
    open,
    ticket,
    intensity,
    rcmd_intensity,
    first_image,
    image_list,
    level,
    hot_level,
    url,
    tagB,
    disable,
    utime,
    nearCity,
    official,
    status_online,
    status_test
  FROM {0};'''.format(table_name)
    elif _poi_type == 'shop':
        cursor.execute('''TRUNCATE base_data.{};'''.format(table_name))
        conn.commit()
        sql = '''REPLACE INTO base_data.{0}
  SELECT
    id,
    name,
    name_en,
    map_info,
    city_id,
    address,
    grade,
    ranking,
    '',
    introduction,
    open,
    intensity,
    rcmd_intensity,
    level,
    first_image,
    image_list,
    hot_level,
    url,
    tagB,
    disable,
    utime,
    nearCity,
    0,
    'Open',
    'Open'
  FROM {0};'''.format(table_name)
    elif _poi_type == 'rest':
        sql = '''REPLACE INTO base_data.{0}
  SELECT
    id,
    name,
    name_en,
    city_id,
    map_info,
    address,
    telphone,
    introduction,
    ranking,
    grade,
    max_price,
    min_price,
    price_level,
    open_time,
    '',
    0,
    michelin_star,
    level,
    first_image,
    image_list,
    hot_level,
    url,
    tagB,
    disable,
    utime,
    nearCity,
    0,
    status_online,
    status_test
  FROM {0};'''.format(table_name)
    else:
        raise TypeError("Unknown Type: {}".format(_poi_type))
    res = cursor.execute(sql)
    cursor.close()
    conn.close()
    logger.debug("[replace base data table][table: {}][replace_count: {}]".format(table_name, res))


def dump_sql_and_upload():
    target_dir = os.path.join('/search/hourong/mv_sql', datetime.datetime.now().strftime("%Y_%m_%d"))
    # 建立 sql 存储文件夹
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # dump data process for bak
    table_time = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    table_names = ['chat_attraction']
    for each_table_name in table_names:
        logger.debug("[start dump data process sql][host: 10.10.242.173][table name: {}]".format(each_table_name))
        command = "mysqldump -h10.10.242.173 -u root -pshizuo0907 data_process {0} > {1}". \
            format(each_table_name,
                   os.path.join(target_dir, each_table_name + '_' + table_time + '.sql'))
        logger.debug("[dump sql][command: {}]".format(command))
        os.system(command)
        logger.debug("[dump data process sql finished][host: 10.10.242.173][table name: {}]".format(each_table_name))

    # dump merge data process data
    for each_table_name in table_names:
        logger.debug("[start dump new data process sql][host: 10.10.228.253][db: poi_merge][table name: {}]".format(
            each_table_name))
        command = "mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 poi_merge {0} > {1}". \
            format(each_table_name,
                   os.path.join(target_dir, each_table_name + '_data_process' + '.sql'))
        logger.debug("[dump new data process sql][command: {}]".format(command))
        os.system(command)
        logger.debug("[dump new data process sql finished][host: 10.10.228.253][db: poi_merge][table name: {}]".format(
            each_table_name))

    # mk base data
    for each_table_name in table_names:
        logger.debug(
            "[start dump base data sql][host: 10.10.228.253][db: base_data][table name: {}]".format(each_table_name))
        command = "cd {0};echo Y|mioji-mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 base_data {1}". \
            format(target_dir, each_table_name)
        logger.debug("[dump base data sql][command: {}]".format(command))
        os.system(command)
        logger.debug(
            "[dump base data sql finished][host: 10.10.228.253][db: base_data][table name: {}]".format(each_table_name))

    # data process load data
    for each_table_name in table_names:
        logger.debug("[start load data process sql][host: 10.10.242.173][table name: {}]".format(each_table_name))
        command = "mysql -h10.10.242.173 -uroot -pshizuo0907 data_process < {1}". \
            format(each_table_name,
                   os.path.join(target_dir, each_table_name + '_data_process' + '.sql'))
        logger.debug("[load data process sql][command: {}]".format(command))
        os.system(command)
        logger.debug("[load data process"
                     " sql finished][host: 10.10.242.173][table name: {}]".format(each_table_name))


if __name__ == '__main__':
    poi_type_list = ['attr']

    for each_poi_type in poi_type_list:
        mk_base_data_final(each_poi_type)
        update_tag_id(each_poi_type)

    dump_sql_and_upload()
