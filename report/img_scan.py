#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/15 上午11:22
# @Author  : Hou Rong
# @Site    :
# @File    : img_scan.py
# @Software: PyCharm
import pandas
from sqlalchemy.engine import create_engine
from service_platform_conn_pool import spider_data_tmp_str
from service_platform_conn_pool import spider_data_tmp_config
from data_source import MysqlSource


def detect():
    conn = create_engine(spider_data_tmp_str)
    table = pandas.read_sql(sql='''SELECT file_name,
  sid,
  url,
  pic_size,
  bucket_name,
  url_md5,
  pic_md5,
  `use`,
  source,
  status,
  date FROM shop_bucket_relation LIMIT 0;''', con=conn)
    table['width'] = ''
    table['height'] = ''
    sql = '''SELECT file_name,
  sid,
  url,
  pic_size,
  bucket_name,
  url_md5,
  pic_md5,
  `use`,
  source,
  status,
  date
FROM shop_bucket_relation
WHERE source IN ('daodao', 'machine', 'NULL') AND `use` = 1 AND pic_size!='NULL';'''

    _count = 0
    for line in MysqlSource(db_config=spider_data_tmp_config, table_or_query=sql,
                            size=1024, is_table=False,
                            is_dict_cursor=True):
        _count += 1
        if _count % 1024 == 0:
            print("now: {}".format(_count))
        width, height = eval(line['pic_size'])
        width = int(width)
        height = int(height)
        line['width'] = width
        line['height'] = height
        if width == height:
            new_row = pandas.DataFrame([line])
            table = table.append(new_row)
    return table


if __name__ == '__main__':
    t = detect()
    t.to_csv("/tmp/shop_img_scan_result.csv")
