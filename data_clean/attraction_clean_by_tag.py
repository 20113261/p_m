# coding=utf-8
import json
import pymysql
from pymysql.cursors import DictCursor
from Config.settings import attr_merge_conf

TASK_TABLE = 'chat_attraction_new'


def get_clean_attraction_tag():
    sql = 'select id,tagid from {0}'.format(TASK_TABLE)
    error_id_set = set()
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    for line in cursor.fetchall():
        miaoji_id = line['id']
        tagid_dict = json.loads(line['tagid'])
        if 'daodao' in tagid_dict:
            tagid = tagid_dict['daodao']
            tag_list = tagid.split('|')
            for tag in tag_list:
                if '游览' in tag.strip():
                    error_id_set.add((miaoji_id,))
    cursor.close()
    return error_id_set


def delete_db(miaoji_id):
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor()
    sql = 'delete from {0} where id=%s'.format(TASK_TABLE)
    res = cursor.executemany(sql, miaoji_id)
    return res


if __name__ == '__main__':
    error_id_set = get_clean_attraction_tag()
    count = 0
    for miaoji_id in error_id_set:
        print(miaoji_id)
        count += 1
        if count > 100:
            break
    print(delete_db(list(error_id_set)))
    print(error_id_set)
