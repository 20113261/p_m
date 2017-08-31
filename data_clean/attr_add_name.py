import pymysql
from pymysql.cursors import DictCursor
from Config.settings import attr_merge_conf

TASK_TABLE = 'data_prepare.attraction_tmp'


def name_problem():
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor(cursor=DictCursor)
    sql = 'select id,name,name_en from {0}'.format(TASK_TABLE)
    cursor.execute(sql)
    datas = []
    for line in cursor.fetchall():
        miaoji_id = line['id']
        name = line['name']
        name_en = line['name_en']
        if name == '':
            name = name_en
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
