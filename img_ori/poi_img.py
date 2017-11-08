#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/27 下午8:58
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_img.py
# @Software: PyCharm
import gevent.monkey

gevent.monkey.patch_all()
import gevent.pool
import logging
import redis
import json
from toolbox.Common import is_legal
from ast import literal_eval
from service_platform_conn_pool import base_data_final_pool, poi_ori_pool, poi_face_detect_pool,spider_data_base_data_pool
from logger import get_logger, func_time_logger
from data_source import MysqlSource
from StandardException import PoiTypeError
from collections import defaultdict

pool = gevent.pool.Pool(size=600)
logger = get_logger("poi_img_merge")
logger.setLevel(logging.DEBUG)
offset = 0

r = redis.Redis(host='10.10.180.145', db=1)

data = []

mail_list = ["hourong@mioji.com", ]
poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'poi_merge'
}

table_name = ''
unid_name = ''


def init_global_name(_poi_type):
    global table_name
    global unid_name
    if _poi_type == 'attr':
        table_name = 'chat_attraction'
        unid_name = 'attr_unid'
    elif _poi_type == 'shop':
        table_name = 'chat_shopping'
        unid_name = 'shop_unid'
    elif _poi_type == 'rest':
        table_name = 'chat_restaurant'
        unid_name = 'rest_unid'
    else:
        raise PoiTypeError(_poi_type)


def get_img(s_sid_set, poi_type, old_img='', old_first_img='', is_official=False):
    """
    Get img str by using source and sid set
    :param is_official: is official or not
    :param old_img: old img list, all img split with |
    :param old_first_img:  old first img, use old sorting
    :param poi_type: poi type, Eg: attr rest shop
    :param s_sid_set: source and sid set
    :return: tuple (new_img str, new_first_img str)
    """
    if not s_sid_set or is_official:
        return old_img, old_first_img

    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    query_sql = '''SELECT
  file_name,
  bucket_name,
  pic_size,
  pic_md5,
  `use`,
  info,
  url
FROM poi_images
WHERE (`source`, `sid`) IN ({});'''.format(','.join(map(lambda x: "('{}', '{}')".format(x[0], x[1]), s_sid_set)))
    _res = cursor.execute(query_sql)
    if not _res:
        return old_img, old_first_img

    max_size = -1
    max_size_img = ''
    file2phash = dict()
    pic_total = set()
    p_hash_dict = defaultdict(list)
    for file_name, bucket_name, pic_size, pic_md5, use, info, url in cursor.fetchall():
        if poi_type == 'shop' and bucket_name not in ('attr_bucket', 'shop_bucket'):
            # shopping img upload to mioji-attr or mioji-shop
            continue
        elif poi_type not in bucket_name:
            # rest img upload to mioji-rest
            # attr img upload to mioji-attr
            continue

        # 生成 pic total，用于判定被过滤的图片是否为人工新添加的图片
        pic_total.add(file_name)

        # 裂图，必须过滤
        if r.get('error_img_{}'.format(file_name)) == '1':
            continue

        # pHash filter
        if url in ('', 'NULL', None):
            # 产品标注图片，不许过滤，直接使用
            file2phash[file_name] = 'USE'
            p_hash_dict["USE"].append(file_name)
            continue
        elif not info:
            # 抓取图片，没有 pHash ，直接过滤
            continue
        else:
            p_hash = json.loads(info)['p_hash']

        # img can be used
        # pic size 为空一般是人工标的图片
        if not is_legal(pic_size):
            if file_name not in old_img:
                continue
            elif str(use) != '1':
                continue
            else:
                # 老图，人工标的，不能过滤
                file2phash[file_name] = 'USE'
                p_hash_dict["USE"].append(file_name)
                continue

        # get max size
        h, w = literal_eval(pic_size)
        h = int(h)
        w = int(w)
        size = h * w
        if size > max_size:
            max_size = size
            max_size_img = file_name

        # use 1
        if str(use) == '1':
            # 过滤规则
            # pixel
            if size < 200000:
                continue

            # scale
            # min scale
            scale = w / h
            if scale < 0.9:
                if w < 500:
                    continue

            # max scale
            if scale > 2.5:
                continue

            p_hash_dict[p_hash].append((file_name, size))

    cursor.close()
    conn.close()

    if poi_type == 'attr':
        # 获取人脸识别数据
        _conn = poi_face_detect_pool.connection()
        _cursor = _conn.cursor()
        query_sql = '''SELECT pic_name
FROM PoiPictureInformation
WHERE is_available=0 AND poi_id IN ({});'''.format(
            ', '.join(
                map(
                    lambda x: "'{}'".format(
                        '###'.join(x) if x[0] != 'online' else x[1]),
                    s_sid_set
                )
            )
        )
        _cursor.execute(query_sql)
        face_detected = set([x[0].split('/')[-1] for x in _cursor.fetchall()])
        _cursor.close()
        _conn.close()
    else:
        face_detected = set()

    # 人工添加图片
    human_pic = p_hash_dict["USE"]

    # 机器图片，同一 pHash 中选取最大的一张图片
    final_pic_dict = {}
    for k, v in p_hash_dict.items():
        pic_res = sorted(v, key=lambda x: x[1], reverse=True)
        if pic_res:
            final_pic_dict[pic_res[0][0]] = k

    old_img_list = old_img.split('|')

    new_img_list = []
    # 按照旧的图片排列顺序增加图片，并去重
    for _old_file_name in old_img_list:
        # 人工添加图片入栈，但无 md5 进行过滤，直接放过 md5 过滤规则
        if (_old_file_name not in pic_total) or (_old_file_name in human_pic):
            # 如果数据合法
            if is_legal(_old_file_name):
                if _old_file_name not in face_detected:
                    if _old_file_name not in new_img_list:
                        # 人工添加图片入栈，但无 md5 进行过滤，直接放过任何过滤规则
                        new_img_list.append(_old_file_name)

        elif _old_file_name in final_pic_dict:
            if is_legal(_old_file_name):
                # 人脸识别过滤
                if _old_file_name not in face_detected:
                    if _old_file_name not in new_img_list:
                        new_img_list.append(_old_file_name)

    # 当新增图片中有原先不存在的图片，按顺序增加图片
    for k, v in final_pic_dict.items():
        if is_legal(v):
            # 人脸识别过滤
            if k not in face_detected:
                if v not in new_img_list:
                    new_img_list.append(k)

    if old_first_img:
        if old_first_img in new_img_list:
            # 当首图没有被下掉的时候，使用原先首图
            new_first_img = old_first_img
            # 从新图片列表中清除 first_img
            new_img_list.remove(old_first_img)
            # 在列表头部增加 first_img
            new_img_list.insert(0, old_first_img)
        else:
            # 否则使用新的首图
            new_first_img = new_img_list[0]
    else:
        if new_img_list:
            new_first_img = new_img_list[0]
        else:
            new_first_img = ''

    if new_img_list == '':
        new_img_list = new_first_img = max_size_img

    new_img = '|'.join(filter(lambda x: is_legal(x), new_img_list))
    return new_img, new_first_img


def get_source_sid_set(uid):
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    query_sql = '''SELECT
  source,
  source_id
FROM {}
WHERE id = '{}';'''.format(unid_name, uid)
    cursor.execute(query_sql)
    _res = set(cursor.fetchall())
    cursor.close()
    conn.close()
    logger.debug("[get source and sid info][uid: {}][source_sid_set: {}]".format(uid, _res))
    return _res


@func_time_logger
def real_update(_data):
    logger.info("[start update img info][count: {}]".format(len(_data)))
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    query_sql = '''UPDATE {} SET first_image=%s,image_list=%s WHERE id=%s;'''.format(table_name)
    _res = cursor.executemany(query_sql, _data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[update data process img info][total: {}][update: {}]".format(len(_data), _res))

    conn = spider_data_base_data_pool.connection()
    cursor = conn.cursor()
    query_sql = '''UPDATE {} SET first_image=%s,image_list=%s WHERE id=%s;'''.format(table_name)
    _res = cursor.executemany(query_sql, _data)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("[update base data img info][total: {}][update: {}]".format(len(_data), _res))
    return _res


def update_img():
    """
    update many img key words
    :return: num of fetch lines
    """
    global data
    if not data:
        return

    _res = real_update(data)
    return _res


def _update_per_uid_img(_uid, _poi_type, _old_img_list, _old_first_img, _official):
    global data
    # init source sid set
    if not is_legal(_old_img_list):
        _old_img_list = ''
    if not is_legal(_old_first_img):
        _old_first_img = ''
    _s_sid_set = get_source_sid_set(_uid)
    _img_list, _first_img = get_img(s_sid_set=_s_sid_set, poi_type=_poi_type, old_img=_old_img_list,
                                    old_first_img=_old_first_img, is_official=(int(_official) == 1))
    logger.debug("[get img info][uid: {}][img_list: {}][first_img: {}]".format(_uid, _img_list, _first_img))
    # 按照 uid 排序，每当 uid 更新后，执行图片更新命令
    data.append((_first_img, _img_list, _uid))


def _img_ori(_poi_type):
    global data
    global offset
    query_sql = '''SELECT
  id,
  image_list,
  first_image,
  official
FROM {}
ORDER BY id
LIMIT {}, 99999999999999;'''.format(table_name, offset)

    _count = 0
    cache = []
    for _uid, _old_img_list, _old_first_img, _official in MysqlSource(poi_ori_config, table_or_query=query_sql,
                                                                      size=500, is_table=False,
                                                                      is_dict_cursor=False):
        cache.append((_uid, _old_img_list, _old_first_img, _official))

    for _uid, _old_img_list, _old_first_img, _official in cache:
        pool.apply_async(_update_per_uid_img, (_uid, _poi_type, _old_img_list, _old_first_img, _official))
        _count += 1
        if _count % 1000 == 0:
            pool.join()
            update_img()
            data = []
            offset += 1000
        update_img()
    pool.join()
    update_img()


def img_ori(_poi_type):
    init_global_name(_poi_type)
    retry_count = 0
    while True:
        retry_count += 1
        try:
            _img_ori(_poi_type)
            break
        except Exception as exc:
            logger.exception(msg="[img ori error][retry count: {}]".format(retry_count), exc_info=exc)


if __name__ == '__main__':
    img_ori('attr')
