#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/23 下午5:11
# @Author  : Hou Rong
# @Site    :
# @File    : get_md5.py
# @Software: PyCharm
import pymongo
import hashlib

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['Backend']['store_task']


def get_md5(src):
    return hashlib.md5(src.encode()).hexdigest()


if __name__ == '__main__':
    pass
    # target_img = {line.strip().split('.')[0] for line in open('/tmp/download_file')}
    # for line in collections.find({
    #     'queue': "file_downloader",
    #     'finished': 1
    # }):
    #     if get_md5(line['args']['target_url']) not in target_img:
    #         print(line['task_token'])
