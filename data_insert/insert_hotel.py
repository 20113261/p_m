#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/14 下午6:15
# @Author  : Chen Xiang
# @Site    :
# @File    : insert_hotel.py
# @Software: PyCharm
import pymysql
import dataset
from sqlalchemy import create_engine
import pandas as pd, numpy as np
from Common import MiojiSimilarCityDict

SQL_DICT = {
    'host': '10.10.180.145',
    'user': 'chenxiang',
    'password': 'chenxiang',
    'charset': 'utf8',
    'db': 'chenxiang'
}
ALL_NULL = ['NULL', 'Null', 'null', None, '', 'None', ' ', np.nan]


# 获取表结构
def get_columns(table_name='hotel'):
    __name = set()
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{1}' AND table_name = '{0}';'''.format(table_name, SQL_DICT['db']))
        for line in cursor.fetchall():
            __name.add(line[0])
    conn.close()
    return __name


def get_max_id() -> int:
    _dict = {}
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute(
            '''SELECT MAX(id) FROM `hotel`''')
        id = cursor.fetchone()[0]
    return id


def generate_id():
    return get_max_id() + 1


def data_insert(row, data_table, hotel_cols_dict, MiojiSimilarCityDict, debug=True):
    data = {}
    # print(52)
    for key in hotel_cols_dict:
        # print(54)
        if hotel_cols_dict[key][0] and row[key] in ALL_NULL:
            # print(56)
            print("主键缺失：{0}".format(key))
            # print(58)
            return
        # print(60)
        if row[key] not in ALL_NULL:
            # print(62)
            if key == '*星级':
                row[key] = {"一": 1, '二': 2, "三": 3, "四": 4, "五": 5}.get(row[key].strip()[0], -1)
            data[hotel_cols_dict[key][1]] = row[key]
            # print(64)

        # 填充字段
        data['id'] = generate_id()
        data['city_mid'] = MiojiSimilarCityDict.get_mioji_city_id(
            (str(row['国家\n中文名']).lower(), str(row['城市\n中文名']).lower()))
    # print(66)
    if debug:
        print(data)
    else:
        data_table.insert(data)


def check_and_modify_columns(key: str, value: str):
    pass


# start = time.time()
conn = create_engine("mysql://reader:miaoji1109@10.10.149.146:3306/private_data", echo=True)
conn_local = create_engine("mysql://chenxiang:chenxiang@10.10.180.145:3306/chenxiang", echo=True)
# table = pd.read_sql_table('chat_attraction', engine, chunksize=2000)
# end = time.time()
# print "time:", end-start
hotel_columns = [(True, '*供应商', 'brand_name'),  # default null
                 (True, '*国家\n英文名', False),
                 (False, '国家\n中文名', 'country'),  # en or cn? d=null
                 (True, '*城市英文名', False),
                 (False, '城市\n中文名', 'city'),  # en or cn?
                 (True, '*酒店\n英文名', 'hotel_name_en'),
                 (False, '酒店\n中文名', 'hotel_name'),
                 (True, '*星级', 'star'),  # d=-1
                 (True, '*是否优先推荐', False),
                 (True, '*地址', 'address'),  # default null
                 (True, '*房型', False),
                 (False, '床型', False),
                 (True, '*可住人数', False),
                 (True, '*是否含早餐', False),
                 (False, '含税间夜价', False),
                 (False, '币种', False),
                 (False, '报价开始时间', False),
                 (False, '报价结束时间', False),
                 (False, '酒店介绍', False),
                 (False, '图片', 'img_url'),
                 (False, '官网', False),
                 (False, '最早入住时间', 'check_in_time'),
                 (False, '最晚退房时间', 'check_out_time'),
                 (False, '源网站', 'all_source'),  # d=null
                 (False, 'url', False)]

"""
pid/map_info/city_mid(d=null)/grade(d=null,float)/img_list/
first_img/description/daodao_index(d=null)/comment_num(d=-1)/status(d=0)/
service/facility/source_sid_get_name(d=null)/hotel_type(d='0')/hotel_type_desc(d='?')/
accepted_cards(d='')/description_en/is_breakfast_free(d='No')/age_limit(d='')/prize(d='')/
brand_tag(d=''),in_city(d=1)/dist_to_city_center(d='-1')/dist_to_top_view(d='')/disable(0 or 1)/
refer(妙计公库hotelID)/rec_priority(0 to 10)
"""


def brand_name_fun():
    pass


def hotel_name_fun():
    pass


def hotel_name_en_fun():
    pass


if __name__ == '__main__':
    # 获取酒店数据
    xlsx_path = '~/Desktop/hotel_trade.csv'
    target_db = 'mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**SQL_DICT)
    target_table = 'hotel'
    # cols = get_columns()

    table = pd.read_csv(
        xlsx_path,
        header=0,
    )
    # 过滤掉无用字段
    hotel_columns_filter = [[primary, table_columns, sql_columns] for (primary, table_columns, sql_columns) in
                            hotel_columns if sql_columns]
    hotel_columns_dict = {table_columns: (primary, sql_columns) for (primary, table_columns, sql_columns) in
                          hotel_columns_filter}
    # 找出table过滤后剩余字段
    table_columns_filter = list(list(zip(*hotel_columns_filter))[1])
    # 找出过滤后剩余数据
    table_filter = table[table_columns_filter]
    data_table = dataset.connect(target_db).get_table(target_table)
    conn = pymysql.connect(**SQL_DICT)
    # hotel_table = pd.read_sql_table("hotel",engine,chunksize = 10000)
    d = MiojiSimilarCityDict.MiojiSimilarCityDict()
    # table向数据库插入数据
    table_filter.apply(data_insert, axis=1, args=(data_table, hotel_columns_dict, d))


    # # 测试20跳数据
    # hotel = pd.read_sql_query("select * from hotel limit 10", conn)
    # # 处理空值
    # hotel = hotel.apply(lambda col:col.apply(lambda x:np.nan if x=='' else x))
    #
    # hotel.to_sql(name='hotel', con=conn_local, if_exists='append', index=False)
    #
    # end = time.time()
    # #print("{0}分{1}秒".format((end-start)/60,(end-start)%60))
