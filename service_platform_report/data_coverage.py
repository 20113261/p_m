#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/25 下午2:27
# @Author  : Hou Rong
# @Site    : 
# @File    : data_coverage.py
# @Software: PyCharm
import pymysql
import datetime
import dataset
from pymysql.cursors import DictCursor
from collections import defaultdict

dev_ip = '10.10.69.170'
dev_user = 'reader'
dev_passwd = 'miaoji1109'
dev_db = 'base_data'

dev_conn = pymysql.connect(host=dev_ip, user=dev_user, charset='utf8', passwd=dev_passwd, db=dev_db)
dev_cursor = dev_conn.cursor()

ori_ip = '10.10.228.253'
ori_user = 'mioji_admin'
ori_password = 'mioji1109'
ori_db_name = 'ServicePlatform'

city_id_count = defaultdict(int)
city_map_info_error_id_set = set()
distance_set = set()

filter_dist = 500


def mk_sql(c_type, table_name):
    hotel_dict = {
        'hotel_name': ('NULL', ''),
        'hotel_name_en': ('NULL', ''),
        'map_info': ('NULL', ''),
        'star': ('NULL', '', '-1'),
        'grade': ('NULL', '', '-1'),
        'review_num': ('NULL', '', '-1'),
        'has_wifi': ('NULL', ''),
        'is_wifi_free': ('NULL', ''),
        'has_parking': ('NULL', ''),
        'is_parking_free': ('NULL', ''),
        'service': ('NULL', ''),
        'img_items': ('NULL', ''),
        'description': ('NULL', ''),
        'accepted_cards': ('NULL', ''),
        'check_in_time': ('NULL', ''),
        'check_out_time': ('NULL', ''),
    }
    poi_dict = {

    }
    qyer_dict = {

    }

    if c_type == 'hotel':
        k_dict = hotel_dict
    else:
        # unknown type
        return None
    keys = ["count(*) as total"]
    for k, v in k_dict.items():
        keys.append(
            '''sum(CASE WHEN `{0}` IN ({1}) OR `{0}` IS NULL THEN 0 ELSE 1 END) AS '{0}' '''.format(
                k, ','.join(map(lambda x: "'{0}'".format(x), v))))
    sql = '''SELECT {0} FROM {1};'''.format(','.join(keys), table_name)
    return sql


def data_coverage():
    local_conn = pymysql.connect(host=ori_ip, user=ori_user, charset='utf8', passwd=ori_password, db=ori_db_name)
    dt = datetime.datetime.now()

    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    report_data = []
    for cand_table in table_list:
        cand_list = cand_table.split('_')
        # 跳过不为 4 的表
        if len(cand_list) != 4:
            continue

        crawl_type, task_type, cand_source, task_tag = cand_list

        if crawl_type != 'detail':
            continue

        report_sql = mk_sql(c_type=task_type, table_name=cand_table)
        if not report_sql:
            # 无法获取查询 sql ，跳过
            continue
        else:
            print(cand_table, report_sql)

        local_cursor = local_conn.cursor(cursor=DictCursor)
        local_cursor.execute(report_sql)
        result = local_cursor.fetchone()
        local_cursor.close()

        for col_name, col_count in result.items():
            report_data.append({
                'tag': task_tag,
                'source': cand_source,
                'type': task_type,
                'col_name': col_name,
                'col_count': col_count,
                'date': datetime.datetime.strftime(dt, '%Y%m%d'),
                'hour': datetime.datetime.strftime(dt, '%H'),
                'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
            })

    # serviceplatform_crawl_report_summary
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    crawl_report_table = db['serviceplatform_data_coverage_summary']

    for each_data in report_data:
        try:
            print(each_data)
            crawl_report_table.upsert(each_data, keys=['tag', 'source', 'type', 'col_name', 'date'],
                                      ensure=None)
        except Exception as exc:
            print(exc)
        print(each_data)
    print('Done')


if __name__ == '__main__':
    data_coverage()
    #     < !--  # error_key = ['全量','数据源错误', '无 name、name_en', "中英文名字相反", "中文名中含有英文名", '坐标错误(NULL)',-->
    # < !--  # '坐标错误(坐标为空或坐标格式错误，除去NULL)', "经纬度重复", '坐标与所属城市距离过远', "距离过远坐标翻转后属于所属城市",-->
    # < !--  # '静态评分异常(评分高于10分)']-->
