#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/19 下午6:22
# @Author  : Hou Rong
# @Site    : 
# @File    : update_city_pic.py
# @Software: PyCharm
import os
import dataset
from collections import defaultdict
from logger import get_logger

logger = get_logger("city_pic_update")

if __name__ == '__main__':
    path = '/Volumes/国家及城市图片/02_已处理图片（技术使用）/美之旅城市图片'
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/base_data?charset=utf8')
    target_table = db['city']
    cid_set = set()
    # 获取需要更新的 city_id
    for line in os.listdir(path):
        f_name = line.strip()
        if f_name == '.DS_Store':
            continue
        cid, _ = f_name.split('_')

        cid_set.add(cid)

    city_img = defaultdict(set)
    # 缓存老的图片信息
    for line in db.query('''SELECT id,new_product_city_pic FROM city WHERE id in ({})'''.format(','.join(
            map(lambda x: "'" + x + "'", cid_set)))):
        _id, _pic = line
        for _each_pic in _pic.split('|'):
            if _each_pic != 'default.jpg':
                city_img[_id].add(_each_pic)

    # 增加新的图片
    for line in os.listdir(path):
        f_name = line.strip()
        if f_name == '.DS_Store':
            continue
        cid, _ = f_name.split('_')

        city_img[cid].add(f_name)

    # 更新图片信息
    for cid, pic_set in city_img.items():
        new_product_pic = '|'.join(pic_set)
        logger.debug("{} => {}".format(cid, new_product_pic))
        target_table.update({
            'id': cid,
            'new_product_city_pic': new_product_pic
        }, keys=['id', ])

    logger.debug(','.join(cid_set))
