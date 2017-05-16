#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/5/9 下午4:58
# @Author  : Hou Rong
# @Site    : 
# @File    : ip_report_by_count.py
# @Software: PyCharm
import pymysql
from datetime import datetime
from collections import defaultdict

sql_dict = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'password': 'hourong',
    'charset': 'utf8',
    'db': 'IP'
}

count_dict = defaultdict(list)
int_split = 5
if __name__ == '__main__':
    _count = 0
    conn = pymysql.connect(**sql_dict)
    with conn as cursor:
        # IP
#         cursor.execute('''SELECT
#   ip_address,
#   count(*)
# FROM ip_used
# GROUP BY ip_address''')
        # local_proxy
        cursor.execute('''SELECT
  local_proxy,
  count(*)
FROM ip_used
GROUP BY local_proxy''')
        for line in cursor.fetchall():
            ip, times = line
            count_dict[times].append(ip)

        x_data = []
        y_data = []
        for k in sorted(count_dict.keys()):
            v = count_dict[k]
            print(str(int_split * k) + ' - ' + str(int_split + int_split * k), len(v))
            x_data.append(str(int_split * k) + ' - ' + str(int_split + int_split * k))
            y_data.append(len(v))
    conn.close()
    print(x_data)
    print(y_data)
