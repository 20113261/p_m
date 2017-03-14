# coding=utf-8
import db_localhost as db

TASK_TABLE = 'data_prepare.attraction_tmp'


def name_problem():
    sql = 'select id,name,name_en from {0}'.format(TASK_TABLE)
    datas = []
    for line in db.QueryBySQL(sql):
        miaoji_id = line['id']
        name = line['name'].replace('这是您的企业吗？', '')
        name_en = line['name_en']
        if name_en in name:
            name = name.replace(name_en, '').strip()
            datas.append((name, miaoji_id))
    return update_db(datas)


def update_db(args):
    sql = 'update {0} set name=%s where id=%s'.format(TASK_TABLE)
    return db.ExecuteSQLs(sql, args)


if __name__ == '__main__':
    print(name_problem())
