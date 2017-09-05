import db_localhost as db
import pymysql
from pymysql.cursors import DictCursor
# todo
ATTR_TABLE = 'data_prepare.attraction_tmp'
REST_TABLE = 'data_prepare.restaurant_tmp'
SHOP_TABLE = 'data_prepare.shopping_tmp'


def get_task(s_type):
    if s_type == 'attr':
        TABLE = ATTR_TABLE
    elif s_type == 'rest':
        TABLE = REST_TABLE
    elif s_type == 'shop':
        TABLE = SHOP_TABLE
    else:
        raise TypeError()
    sql = 'select id from {0} where map_info="" or map_info="NULL"'.format(TABLE)
    for line in db.QueryBySQL(sql):
        yield line['id']


def delete_db(args, s_type):
    if s_type == 'attr':
        TABLE = ATTR_TABLE
    elif s_type == 'rest':
        TABLE = REST_TABLE
    elif s_type == 'shop':
        TABLE = SHOP_TABLE
    else:
        raise TypeError()
    sql = 'delete from {0} where id=%s'.format(TABLE)
    return db.ExecuteSQLs(sql, args)


if __name__ == '__main__':
    # -------- Variables ---------

    S_TYPE_LIST = ['attr', 'rest', 'shop']

    # ----------------------------

    for S_TYPE in S_TYPE_LIST:
        data = []
        for m_id in get_task(S_TYPE):
            data.append((m_id,))
        print(delete_db(data, S_TYPE))
