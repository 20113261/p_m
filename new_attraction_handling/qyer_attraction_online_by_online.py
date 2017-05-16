#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/5/15 下午7:18
# @Author  : Hou Rong
# @Site    : 
# @File    : qyer_attraction_online.py
# @Software: PyCharm
import dataset
from collections import defaultdict

dev_db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.114.35/attr_merge?charset=utf8')
online_db = dataset.connect('mysql+pymysql://reader:miaoji1109@10.10.69.170/base_data?charset=utf8')
target_db = dataset.connect('mysql+pymysql://hourong:hourong@localhost/data_prepare?charset=utf8')

if __name__ == '__main__':
    target_table = target_db['chat_attraction']
    name_dict = defaultdict(set)
    id_set = set()

    # 生成已上线名称字典
    for row in online_db.query('''SELECT
  id,
  name,
  name_en,
  city_id
FROM chat_attraction;'''):
        name_dict[row['city_id']].add(row['name'].lower())
        name_dict[row['city_id']].add(row['name_en'].lower())
        id_set.add(row['id'])

    # 查询所有有关 qyer 的开发库景点
    for row in dev_db.query('''SELECT *
FROM chat_attraction
WHERE data_source LIKE '%qyer%' AND map_info != '' AND map_info != 'NULL' AND map_info != '0' AND name != '' AND
      name != 'NULL' AND name != '0';'''):
        if row['id'] in id_set:
            continue
        if row['city_id'] in name_dict:
            if row['name'].lower() not in name_dict[row['city_id']] or row['name_en'].lower() not in name_dict[
                row['city_id']]:
                # 数据修改部分
                if row['norm_tagid'] in ('', 'NULL', '0') and row['intensity'] in ('', 'NULL', '0'):
                    row['intensity'] = '2-1:3600_-1.0_no'
                    row['rcmd_intensity'] = '2-1'

                if row['open'] in ('', 'NULL', '0'):
                    row['open'] = '<*><*><00:00-23:55><SURE>'

                # 数据插入部分
                target_table.insert(row)
        else:
            # 数据修改部分
            if row['norm_tagid'] in ('', 'NULL', '0') and row['intensity'] in ('', 'NULL', '0'):
                row['intensity'] = '2-1:3600_-1.0_no'
                row['rcmd_intensity'] = '2-1'

            if row['open'] in ('', 'NULL', '0'):
                row['open'] = '<*><*><00:00-23:55><SURE>'

            target_table.insert(row)
