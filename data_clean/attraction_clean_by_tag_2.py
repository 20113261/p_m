# coding=utf-8
import json
import re

import db_localhost as db

TASK_TABLE = 'data_prepare.attraction_tmp'

split_pattern = re.compile('[|与]')


def get_tagid_dict():
    tag_tag_en_dict = {}
    sql = 'select tag,tag_en from tag.attraction_tagS'
    for line in db.QueryBySQL(sql):
        tag = line['tag']
        tag_en = line['tag_en']
        tag_tag_en_dict[tag] = tag_en
    return tag_tag_en_dict


def get_task():
    tag_dict = get_tagid_dict()
    error_id_set = set()
    sql = 'select id,tagid,data_source from {0}'.format(TASK_TABLE)
    count = 0
    for line in db.QueryBySQL(sql):
        miaoji_id = line['id']
        tagid = line['tagid']
        data_source = line['data_source']
        if 'daodao' not in data_source:
            continue
        norm_tag_list = []
        nrom_tag_en_list = []
        for raw_tag in split_pattern.split(json.loads(tagid)['daodao']):
            tag = raw_tag.strip()
            if tag in tag_dict:
                norm_tag_list.append(tag)
                nrom_tag_en_list.append(tag_dict[tag])
        norm_tag = '|'.join(norm_tag_list)
        count += 1
        if norm_tag == '':
            error_id_set.add((miaoji_id,))
    return error_id_set


def delete_db(error_id_set):
    sql = 'delete from {0} where id=%s'.format(TASK_TABLE)
    return db.ExecuteSQLs(sql, error_id_set)


if __name__ == '__main__':
    error_id_set = get_task()
    # print len(error_id_set)
    print(delete_db(error_id_set))
