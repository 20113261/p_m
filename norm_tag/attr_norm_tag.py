# coding=utf-8
import json
import re
import pymysql
from pymysql.cursors import DictCursor
from Config.settings import local_tag_conf
from .lang_convert import tradition2simple

ATTR_TABLE = 'data_prepare.attraction_tmp'

split_pattern = re.compile('[|与]')


def get_tagid_dict():
    tag_tag_en_dict = {}
    sql = 'select tag,tag_en from attraction_tagS'
    conn = pymysql.connect(**local_tag_conf)
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    for line in cursor.fetchall():
        tag = line['tag']
        tag_en = line['tag_en']
        tag_tag_en_dict[tag] = tag_en
    return tag_tag_en_dict


tag_dict = get_tagid_dict()


def get_norm_tag(tag_id):
    norm_tag_list = []
    norm_tag_en = []
    lines = tradition2simple(tag_id['daodao']).decode()
    for raw_tag in split_pattern.split(lines):
        tag = raw_tag.strip()
        if tag in tag_dict:
            norm_tag_list.append(tag)
            norm_tag_en.append(tag_dict[tag])
    norm_tag = '|'.join(norm_tag_list)
    norm_tag_en = '|'.join(norm_tag_en)
    return norm_tag, norm_tag_en


def get_datas():
    datas = []
    sql = 'select id,tagid,data_source from ' + ATTR_TABLE
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
        norm_tag_en = '|'.join(nrom_tag_en_list)
        count += 1
        if norm_tag != '':
            data = (norm_tag, norm_tag_en, miaoji_id)
            datas.append(data)
    return datas


def update_db(datas):
    sql = 'update ' + ATTR_TABLE + ' set norm_tagid=%s,norm_tagid_en=%s where id=%s'
    return db.ExecuteSQLs(sql, datas)


if __name__ == '__main__':
    datas = get_datas()
    print(update_db(datas))
