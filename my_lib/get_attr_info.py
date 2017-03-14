# coding=utf8
import db_localhost
import pymysql
from pymysql.cursors import DictCursor
from mysql_config import attr_merge

db = db_localhost


def get_tp_attr_info_by_city_id(city_id):
    conn = pymysql.connect(**attr_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from attr where city_id=%s and source="daodao"'
        cursor.execute(sql, (city_id,))
        res = cursor.fetchall()
    conn.close()
    return res


# def get_qyer_attr_info(source_city_id):
#     sql = 'select * from qyer where source_city_id=%s and (cateid = "景点" or cateid = "景点观光")' % source_city_id
#     results = db.QueryBySQL(sql)
#     return results


def get_qyer_attr_info_by_city_id(city_id):
    conn = pymysql.connect(**attr_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from attr where city_id=%s and source="qyer" and (cateid = "景点" or cateid = "景点观光")'
        cursor.execute(sql, (city_id,))
        res = cursor.fetchall()
    conn.close()
    return res


if __name__ == '__main__':
    result = get_qyer_attr_info_by_city_id('10001')
    print(len(result))
