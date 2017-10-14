#!/usr/bin/env python
# encoding: utf-8
import pymysql
from pymysql.cursors import SSDictCursor, SSCursor


class MysqlSource:
    """
    数据库data源
    """

    def __init__(self, db_config, table_or_query='', size=500, is_table=True, is_dict_cursor=False):
        self._db_config = db_config
        if is_dict_cursor:
            self._db_config['cursorclass'] = SSDictCursor
        else:
            self._db_config['cursorclass'] = SSCursor
        self._size = size
        self._table = table_or_query
        if is_table:
            self._sql = 'select * from {0}'.format(self._table)
        else:
            self._sql = table_or_query

    def __iter__(self):
        return cursor_gen(pymysql.Connect(**self._db_config), self._sql, self._size)


def cursor_gen(con, query, size):
    try:
        con = con
        with con.cursor() as cursor:
            cursor.execute(query)
            print('after excute')
            rows = cursor.fetchmany(size)
            print('after fetchmany', len(rows))
            while rows:
                for r in rows:
                    yield r
                print('b fetchmany')
                rows = cursor.fetchmany(size)
                print('after fetchmany')
    except Exception as e:
        import traceback
        print(traceback.format_exc())
    finally:
        print('finally')
        if con:
            con.close()
