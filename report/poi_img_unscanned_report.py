#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/7 下午3:52
# @Author  : Hou Rong
# @Site    : 
# @File    : poi_img_unscanned_report.py
# @Software: PyCharm

"""
set 1: poi_images online attr_bucket use 1 has url
set 2: PoiPicInformation poi_id no img
"""
from data_source import MysqlSource
from service_platform_conn_pool import poi_face_detect_pool
from my_logger import get_logger

logger = get_logger("poi_img_not_scanned_report")

poi_ori_config = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'BaseDataFinal'
}


def get_poi_pic_detect(poi_id):
    conn = poi_face_detect_pool.connection()
    cursor = conn.cursor()
    query_sql = '''SELECT pic_name
FROM PoiPictureInformation
WHERE poi_id = '{}';'''.format(poi_id)
    cursor.execute(query_sql)
    res = set([line[0].split('/')[-1] for line in cursor.fetchall()])
    cursor.close()
    conn.close()
    return res


def task():
    query_sql = '''SELECT
  sid,
  file_name,
  bucket_name
FROM poi_images
WHERE source = 'online' AND bucket_name LIKE '%attr%' AND sid LIKE 'v%' AND `use` = 1
ORDER BY sid;'''

    old_poi_id = None
    img_name_set = set()
    _count = 0

    result_f = open('/tmp/img_res_new', mode='w')

    for line in MysqlSource(poi_ori_config, table_or_query=query_sql,
                            size=10000, is_table=False,
                            is_dict_cursor=True):
        _count += 1
        if _count % 3000 == 0:
            logger.debug("[now count: {}]".format(_count))

        if 'attr' not in line['bucket_name'] and not line['sid'].startswith('v'):
            continue

        # 先获取 poi id
        poi_id = line['sid']

        # id 变更后，查找图片，重新生成
        if poi_id != old_poi_id:
            if old_poi_id is not None:
                has_detected_pic_file = get_poi_pic_detect(old_poi_id)
                lost_img = (img_name_set - has_detected_pic_file)
                for i in lost_img:
                    logger.debug("[img not detected][poi_id: {}][img: {}]".format(old_poi_id, i))
                    result_f.write('{}###{}\n'.format(old_poi_id, i))
            old_poi_id = poi_id
            img_name_set = set()

        file_name = line['file_name']
        img_name_set.add(file_name)


if __name__ == '__main__':
    task()
