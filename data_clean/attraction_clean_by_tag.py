# coding=utf-8
import json

import db_localhost as db

TASK_TABLE = 'data_prepare.attraction_tmp'


def get_clean_attraction_tag():
    sql = 'select id,tagid from {0}'.format(TASK_TABLE)
    error_id_set = set()
    for line in db.QueryBySQL(sql):
        miaoji_id = line['id']
        tagid_dict = json.loads(line['tagid'])
        if 'daodao' in tagid_dict:
            tagid = tagid_dict['daodao']
            tag_list = tagid.split('|')
            for tag in tag_list:
                if '游览' in tag.strip():
                    error_id_set.add((miaoji_id,))
    return error_id_set


def delete_db(miaoji_id):
    sql = 'delete from {0} where id=%s'.format(TASK_TABLE)
    return db.ExecuteSQLs(sql, miaoji_id)


if __name__ == '__main__':
    error_id_set = get_clean_attraction_tag()
    count = 0
    for miaoji_id in error_id_set:
        print(miaoji_id)
        count += 1
        if count > 100:
            break
    print(delete_db(list(error_id_set)))
