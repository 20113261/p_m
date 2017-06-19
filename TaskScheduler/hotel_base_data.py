#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/19 下午5:22
# @Author  : Hou Rong
# @Site    : 
# @File    : hotel_base_data.py
# @Software: PyCharm

import re
from TaskScheduler.TaskInsert import InsertTask


def get_task_hotel_raw():
    # conn = pymysql.connect(user='hourong', passwd='hourong', db='hotel_adding', charset="utf8")
    # cursor = conn.cursor(cursor=DictCursor)
    # sql = 'select source,source_id,hotel_url,city_id from hotelinfo_static_data where source="booking" and (grade="NULL" or grade="-1" or grade="" or grade is null);'
    # cursor.execute(sql)
    # for line in cursor.fetchall():
    #     other_info = {
    #         'source_id': line['source_id'],
    #         'city_id': line['city_id']
    #     }
    #     yield line['source'], line['hotel_url'], other_info
    # conn.close()
    # # todo 1
    # obj = pickle.load(open('/root/data/task/hotel_total_info.pk', 'rb'))
    # for line in obj:
    #     if line[0] != 'ctrip':
    #         continue
    #     other_info = {
    #         'source_id': line[1],
    #         'city_id': line[2]
    #     }
    #     yield line[0], line[3], other_info
    # todo 2
    # conn = pymysql.connect(user='hourong', passwd='hourong', db='hotel_pre', charset="utf8")
    # cursor = conn.cursor(cursor=DictCursor)
    # sql = 'select source,source_hotelid,hotel_url,city from data_for_crawling where hotel_url!="NULL" and hotel_url is not null and hotel_url!="" and source in ("booking", "agoda", "expedia", "elong", "hotels", "hoteltravel")'
    # cursor.execute(sql)
    # for line in cursor.fetchall():
    #     other_info = {
    #         'source_id': line['source_hotelid'],
    #         'city_id': line['city']
    #     }
    #     if line['hotel_url'].lower() not in ('', 'null', '0'):
    #         yield line['source'], line['hotel_url'], other_info
    # conn.close()
    # todo 3
    # conn = pymysql.connect(user='hourong', passwd='hourong', db='hotel', charset="utf8")
    # cursor = conn.cursor(cursor=DictCursor)
    # # sql = 'select source,source_id,hotel_url,city_id from hotelinfo_static_data where source="expedia"'
    # sql = "select * from hotelinfo_static_data where source='hotels'"
    # cursor.execute(sql)
    # for line in cursor.fetchall():
    #     other_info = {
    #         'source_id': line['source_id'],
    #         'city_id': line['city_id']
    #     }
    #     yield line['source'], line['hotel_url'], other_info
    # conn.close()
    # todo 4
    # f = open('/root/data/task/task_list_0512')
    # for line in f:
    #     try:
    #         source, source_id, city_id, hotel_url = line.strip().split('\t')
    #     except:
    #         print('Hello')
    #     other_info = {
    #         'source_id': source_id,
    #         'city_id': city_id
    #     }
    #     yield source, hotel_url, other_info
    # todo 5
    # import pandas
    # table = pandas.read_csv('/root/data/data_input/source_sid_cid', sep='\t')
    # import pdb
    # pdb.set_trace()
    f = open('/root/data/task/source_sid_cid_for_hotels')
    for line in f:
        try:
            source, source_id, city_id, url = line.strip().split('\t')
        except:
            print('Hello')
            continue
        if url.lower() not in ('', 'null', 'http', 'http:', 'http://', 'https', 'https:', 'https://'):
            other_info = {
                'source_id': source_id,
                'city_id': city_id
            }
            try:
                if source not in (
                        'agoda', 'booking', 'cheaptickets', 'ctrip', 'ebookers', 'elong', 'expedia', 'hotels',
                        'hoteltravel',
                        'orbitz', 'travelocity'):
                    continue
                elif source == 'hotels':
                    hotel_id = re.findall('hotel-id=(\d+)', url)[0]
                    hotel_url = 'http://zh.hotels.com/hotel/details.html?hotel-id=' + hotel_id
                elif source in ('booking', 'ctrip', 'expedia', 'travelocity', 'orbitz', 'ebookers', 'cheaptickets'):
                    hotel_url = url.split('?')[0]
                elif source == 'elong':
                    hotel_url = 'http://hotel.elong.com/{0}/'.format(source_id)
                elif source == 'hoteltravel':
                    hotel_url = 'http://www.hoteltravel.com/cn/' + source_id
                else:
                    hotel_url = url

                yield source, hotel_url, other_info
            except:
                continue


if __name__ == '__main__':
    task_name = 'hotel_base_data_170619'
    it = InsertTask(worker='hotel_base_data', task_name=task_name)

    for source, hotel_url, other_info in get_task_hotel_raw():
        args = {'source': source, 'hotel_url': hotel_url, 'other_info': other_info, 'part': task_name}
        it.insert_task(args=args)
