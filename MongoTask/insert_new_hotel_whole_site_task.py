#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/20 上午9:45
# @Author  : Hou Rong
# @Site    : 
# @File    : insert_new_hotel_whole_site_task.py
# @Software: PyCharm
import pymongo
import pymongo.errors
import datetime

from Common.GetMd5 import get_token
from Common.Utils import is_legal, modify_url

client = pymongo.MongoClient(host='10.10.231.105')

src_collections = client['FullSiteSpider']['HotelFullSite']
collections = client['Task']['NewHotelTask']


def insert_many_task(d: list, is_end: bool = False) -> int:
    """
    insert many task into mongo
    :param d: data
    :param is_end:
    :return: query nums
    """
    if d and (len(d) % 10000 == 0 or is_end):
        res = []
        try:
            res = collections.insert(d, continue_on_error=True) or []
        except pymongo.errors.DuplicateKeyError:
            pass
        d = []
        # print('New', len(data))
        # print('Insert', len(res))
        return res
    return 0





if __name__ == '__main__':
    pass
    # data = []
    #
    # count = 0
    # collections.remove()
    # for line in src_collections.find():
    #     count += 1
    #     if count % 1000 == 0:
    #         print('Now', count)
    #     source_id = line['parent_info']['id']
    #     pdf_url_list = line['pdf_url']
    #     img_url_list = line['img_url']
    #     for img_url in img_url_list:
    #         if is_legal(img_url):
    #             if 'logo' not in img_url.lower():
    #                 modified_url = modify_url(img_url)
    #                 if modified_url:
    #                     args = {
    #                         'mid': source_id,
    #                         'type': 'img',
    #                         'source_url': modified_url
    #                     }
    #
    #                     data.append({
    #                         'args': args,
    #                         'task_token': get_token(args),
    #                         'used_times': 0,
    #                         'finished': 0,
    #                         'utime': datetime.datetime.now()
    #                     })
    #
    #                     insert_many_task(data)
    #
    #     for pdf_url in pdf_url_list:
    #         if is_legal(pdf_url):
    #             modified_url = modify_url(pdf_url)
    #             if modified_url:
    #                 args = {
    #                     'mid': source_id,
    #                     'type': 'pdf',
    #                     'source_url': modified_url
    #                 }
    #
    #                 data.append({
    #                     'args': args,
    #                     'task_token': get_token(args),
    #                     'used_times': 0,
    #                     'finished': 0,
    #                     'utime': datetime.datetime.now()
    #                 })
    #
    #                 insert_many_task(data)
    #
    # print(count)
    # insert_many_task(data, True)
