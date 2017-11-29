#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午3:34
# @Author  : Hou Rong
# @Site    : 
# @File    : img_down.py
# @Software: PyCharm
import json
from service_platform_conn_pool import spider_data_base_data_pool, base_data_final_pool
from logger import get_logger

logger = get_logger("remove_img")


def update_info(vid, first_img, img_list):
    conn = spider_data_base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''UPDATE chat_attraction
SET first_image = %s, image_list = %s
WHERE id = %s;''', (first_img, img_list, vid))
    conn.commit()
    cursor.close()
    conn.close()


def update_img_info(img_name):
    # 更新图片信息
    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT `info`
FROM poi_images
WHERE file_name = %s;''', (img_name,))
    info = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if info:
        _i = json.loads(info)
    else:
        _i = {}

    _i['delete_reason'] = "图片质量低，手动取消"

    conn = base_data_final_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''UPDATE poi_images
SET `use` = 0, info = %s
WHERE file_name = %s;''', (json.dumps(_i), img_name,))
    conn.commit()
    cursor.close()
    conn.close()

    logger.info("[new info][info: {}]".format(json.dumps(_i)))


def down_img(vid, img_name):
    conn = spider_data_base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT
  first_image,
  image_list
FROM chat_attraction
WHERE id = %s''', (vid,))
    res = cursor.fetchone()
    cursor.close()
    conn.close()

    first_img, img_list = res

    img_res = img_list.split('|')
    if img_name in img_res:
        img_res.remove(img_name)
    else:
        logger.info("[img not in list: {}][img_list: {}]".format(img_name, img_list))
        return first_img, img_list

    new_img_list = '|'.join(img_res).strip()
    if first_img == img_name:
        new_first_img = new_img_list[0]
    else:
        new_first_img = first_img
    logger.info(
        "[remove img][img: {}][new_first_img: {}][new_img_list: {}]".format(img_name, new_first_img, new_img_list))
    return new_first_img, new_img_list


def task(vid, img_name):
    f_img, img_list = down_img(vid, img_name)
    update_info(vid, f_img, img_list)
    update_img_info(img_name)


if __name__ == '__main__':
    task_info = [
        ('v740906', 'beb78198d4a41bd75ca523769d397f32.jpg'),
        ('v740860', '32f01f5b9b01741f56bf3eeba381d464.jpg'),
        ('v740909', 'cf2426d495d07d110b27349b7e3ef14e.jpg')
    ]

    for _vid, _img_name in task_info:
        task(_vid, _img_name)
