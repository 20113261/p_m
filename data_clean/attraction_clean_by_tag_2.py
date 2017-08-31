# coding=utf-8
import json
import re
import pymysql
from pymysql.cursors import DictCursor

from Config.settings import attr_merge_conf, local_tag_conf

TASK_TABLE = 'chat_attraction_new'

split_pattern = re.compile('[|与]')


def get_tagid_dict():
    tag_tag_en_dict = {}
    conn = pymysql.connect(**local_tag_conf)
    cursor = conn.cursor(cursor=DictCursor)
    sql = 'select tag,tag_en from attraction_tagS'
    cursor.execute(sql)
    for line in cursor.fetchall():
        tag = line['tag']
        tag_en = line['tag_en']
        tag_tag_en_dict[tag] = tag_en
    return tag_tag_en_dict


def get_task():
    tag_dict = get_tagid_dict()
    error_id_set = set()
    conn = pymysql.connect(**attr_merge_conf)
    sql = 'select id,tagid,data_source from {0}'.format(TASK_TABLE)
    count = 0
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    for line in cursor.fetchall():
        miaoji_id = line['id']
        tagid = line['tagid']
        data_source = line['data_source']
        if 'daodao' not in data_source:
            continue
        norm_tag = []
        norm_tag_en = []
        for raw_tag in split_pattern.split(json.loads(tagid)['daodao']):
            tag = raw_tag.strip()
            if tag in tag_dict:
                norm_tag.append(tag)
                norm_tag_en.append(tag_dict[tag])
        norm_tag = '|'.join(norm_tag)
        count += 1
        if norm_tag == '':
            error_id_set.add((miaoji_id,))
    return error_id_set


def delete_db(error_id_set):
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor()
    sql = 'delete from {0} where id=%s'.format(TASK_TABLE)
    res = cursor.executemany(sql, error_id_set)
    return res


if __name__ == '__main__':
    error_id_set = get_task()
    # print len(error_id_set)
    print(delete_db(error_id_set))
    print(error_id_set)
