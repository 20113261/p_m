#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/26 下午2:04
# @Author  : Hou Rong
# @Site    : 
# @File    : create_view.py
# @Software: PyCharm
import pymysql

if __name__ == '__main__':
    local_conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', charset='utf8', passwd='mioji1109',
                                 db='ServicePlatform')

    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    table_name = list(
        filter(lambda x: x.endswith('20170925d') and x.startswith('detail_'),
               table_list))

    for name in table_name:
        _report_type, _crawl_type, _task_source, _tag_id = name.split('_')

        if _crawl_type == 'hotel':
            detail_name = '_'.join(['detail', _crawl_type, _task_source, _tag_id])
            list_name = '_'.join(['list', _crawl_type, _task_source, _tag_id])
            view_name = '_'.join(['view', _crawl_type, _task_source, _tag_id])

            view_sql = '''DROP VIEW IF EXISTS {0};
        CREATE VIEW {0} AS
          SELECT
            hotel_name,
            hotel_name_en,
            {1}.source,
            {1}.source_id,
            brand_name,
            {1}.map_info,
            address,
            city.name    AS city,
            country.name AS country,
            {2}.city_id,
            postal_code,
            star,
            {1}.grade,
            review_num,
            has_wifi,
            is_wifi_free,
            has_parking,
            is_parking_free,
            service,
            img_items,
            description,
            accepted_cards,
            check_in_time,
            check_out_time,
            {1}.hotel_url,
            update_time,
            continent,
            {2}.country_id
          FROM {1}
            JOIN {2}
              ON {1}.source = {2}.source AND
                 {1}.source_id = {2}.source_id
            JOIN base_data.city ON {2}.city_id = base_data.city.id
            JOIN base_data.country ON base_data.city.country_id = base_data.country.mid;'''.format(view_name,
                                                                                                   detail_name,
                                                                                                   list_name)
            local_cursor = local_conn.cursor()
            local_cursor.execute(view_sql)
            local_conn.commit()
            local_cursor.close()
        else:
            print(name)
