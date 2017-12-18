#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/15 下午8:57
# @Author  : Hou Rong
# @Site    : 
# @File    : check_lost_url.py
# @Software: PyCharm
import re
from service_platform_conn_pool import fetchall, base_data_pool, service_platform_pool


def get_all_url():
    sql = '''SELECT id,name,name_en,json_extract(url, '$.qyer')
FROM chat_attraction
WHERE json_extract(url, '$.qyer') IS NOT NULL;'''
    _set = set()
    _dict = {}
    for line in fetchall(base_data_pool, sql):
        _url_list = re.findall('place.qyer.com/poi/([\s\S]+)/', line[-1])
        if _url_list:
            _url_id = _url_list[-1]
            _set.add(_url_id)
            _dict[_url_id] = line
    return _set, _dict


def get_all_new_url():
    sql = '''SELECT url
FROM qyer_whole_world;'''
    _set = set()
    for line in fetchall(service_platform_pool, sql):
        _url_id = re.findall('place.qyer.com/poi/([\s\S]+)/', line[-1])[0]
        _set.add(_url_id)
    return _set


if __name__ == '__main__':
    import pandas

    s_n = get_all_new_url()
    s, r_d = get_all_url()

    data = []
    for url in s:
        if url not in s_n:
            # l = list(r_d[url])
            # l.append(url)
            # data.append(l)
            d = list(r_d[url])
            d[-1] = d[-1][1:-1]
            data.append(d)
    df = pandas.DataFrame(columns=["id", "name", "name_en", "url"], data=data)
    df.to_csv('/tmp/result.csv')
    print(df.sample(10))
