# coding=utf8
import pymysql
from pymysql.cursors import DictCursor

shop_merge = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'passwd': 'hourong',
    'charset': 'utf8',
    'db': 'shop_merge'
}


# def get_tp_shop_info(gid):
#     conn = pymysql.connect(**shop_merge)
#     with conn.cursor(cursor=DictCursor) as cursor:
#         sql = 'select * from shop where city_id=%s and source="daodao"'
#         cursor.execute(sql, (city_id,))
#         res = cursor.fetchall()
#     conn.close()
#     return res
#
#     sql = 'select * from tp_shop_basic_0801 where source_city_id="%s"' % gid
#     return db.QueryBySQL(sql)


def get_tp_shop_info_by_city_id(city_id):
    conn = pymysql.connect(**shop_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from shop where city_id=%s and source="daodao"'
        cursor.execute(sql, (city_id,))
        res = cursor.fetchall()
    conn.close()
    return res
