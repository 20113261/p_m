# coding=utf-8
import pymysql
from pymysql.cursors import DictCursor
from Config.settings import attr_merge_conf

TASK_TABLE = 'chat_attraction_new'


def name_problem():
    sql = 'select id,name,name_en from {0}'.format(TASK_TABLE)
    datas = []
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    for line in cursor.fetchall():
        miaoji_id = line['id']
        name = line['name'].replace('这是您的企业吗？', '')
        name_en = line['name_en']
        if name_en in name:
            name = name.replace(name_en, '').strip()
            datas.append((name, miaoji_id))
    return update_db(datas)


def update_db(args):
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor()
    sql = 'update {0} set name=%s where id=%s'.format(TASK_TABLE)
    res = cursor.executemany(sql, args)
    conn.close()
    return res


if __name__ == '__main__':
    print(name_problem())
