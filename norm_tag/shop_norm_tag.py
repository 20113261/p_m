# coding=utf-8
import json
import re
import pymysql
from pymysql.cursors import DictCursor
from Config.settings import local_tag_conf
from .lang_convert import tradition2simple

SHOP_TABLE = 'data_prepare.shopping_tmp'
split_pattern = re.compile('[|与,]')


def get_tagid_dict():
    conn = pymysql.connect(**local_tag_conf)
    cursor = conn.cursor(cursor=DictCursor)
    tag_tag_en_dict = {}
    sql = 'select tag,tag_en from shopping_tagS'
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
    unknown = []
    lines = tradition2simple(tag_id).decode()
    for raw_tag in split_pattern.split(lines):
        tag = raw_tag.strip()
        if tag in tag_dict:
            norm_tag_list.append(tag)
            norm_tag_en.append(tag_dict[tag])
    norm_tag = '|'.join(norm_tag_list)
    norm_tag_en = '|'.join(norm_tag_en)
    if norm_tag == '' and tag_id != '':
        print(tag_id)
    return norm_tag, norm_tag_en
