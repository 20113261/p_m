# coding=utf8
# import db_localhost as db
import pymysql
from pymysql.cursors import DictCursor

# def get_tp_rest_info(gid):
#     sql = 'select * from tp_rest_basic_0801 where source_city_id="%s"' % gid
#     return db.QueryBySQL(sql)
#
#
# def get_tp_rest_info_by_city_id(city_id):
#     sql = 'select * from tp_rest_basic_0214 where city_id="{}"'.format(city_id)
#     return db.QueryBySQL(sql)


rest_merge = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'passwd': 'hourong',
    'charset': 'utf8',
    'db': 'rest_merge'
}


def get_tp_rest_info_by_city_id(city_id):
    conn = pymysql.connect(**rest_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from rest where city_id=%s and source="daodao"'
        cursor.execute(sql, (city_id,))
        res = cursor.fetchall()
    conn.close()
    return res


if __name__ == '__main__':
    rest_info = get_tp_rest_info_by_city_id('10001')
    print('Hello World')
