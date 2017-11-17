#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/16 下午8:57
# @Author  : Hou Rong
# @Site    : 
# @File    : huantaoyou_report.py
# @Software: PyCharm
import pandas
import dataset
from collections import defaultdict

spider_base_tmp_wanle_str = "mysql+pymysql://mioji_admin:mioji1109@10.10.230.206/tmp_wanle?charset=utf8"
spider_base_tmp_wanle_test_str = "mysql+pymysql://mioji_admin:mioji1109@10.10.230.206/tmp_wanle_test?charset=utf8"

country_set = set()
data = defaultdict(int)
for n, t in [('景点门票', 'view_ticket'), ('演出赛事', 'play_ticket'), ('特色活动', 'activity_ticket')]:
    sql = '''SELECT
          tmp.country.mid,
          tmp.country.name,
          count(*)
        FROM {0}
          JOIN tmp.city ON {0}.city_id = tmp.city.id
          JOIN tmp.country ON tmp.city.country_id = tmp.country.mid
        WHERE {0}.disable = 0
        GROUP BY tmp.country.mid;'''.format(t)
    db = dataset.connect(spider_base_tmp_wanle_str)
    values = defaultdict(int)
    for line in db.query(sql):
        values[line['name']] = line['count(*)']
    data[n] = values
    country_set.update(data[n].keys())

columns = pandas.MultiIndex.from_product([['景点门票', '演出赛事', '特色活动'], ['所属票数', '占全部欢逃游产品的票数的百分比']])
country_list = list(country_set)
table = pandas.DataFrame(index=list(country_set), columns=columns)
for key, values in data.items():
    value_list = [values[c] for c in country_list]
    table[(key, '所属票数')] = value_list
    table[(key, '占全部欢逃游产品的票数的百分比')] = round(table[(key, '所属票数')] * 100 / table[(key, '所属票数')].sum(), 2)

print(table)
