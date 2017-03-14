# coding=utf-8
import json
import re

import db_localhost as db

SHOP_TABLE = 'data_prepare.shopping_tmp'
split_pattern = re.compile('[|与]')


def get_tagid_dict():
    tag_tag_en_dict = {}
    sql = 'select tag,tag_en from tag.shopping_tagS'
    for line in db.QueryBySQL(sql):
        tag = line['tag']
        tag_en = line['tag_en']
        tag_tag_en_dict[tag] = tag_en
    return tag_tag_en_dict


def get_datas():
    tag_dict = get_tagid_dict()
    datas = []
    sql = 'select id,tagid,data_source from ' + SHOP_TABLE
    count = 0
    for line in db.QueryBySQL(sql):
        miaoji_id = line['id']
        tagid = line['tagid']
        data_source = line['data_source']
        if data_source == 'qyer':
            continue
        norm_tag_list = []
        nrom_tag_en_list = []
        for raw_tag in split_pattern.split(json.loads(tagid).get('daodao', '')):
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
    sql = 'update ' + SHOP_TABLE + ' set norm_tagid=%s,norm_tagid_en=%s where id=%s'
    return db.ExecuteSQLs(sql, datas)


if __name__ == '__main__':
    datas = get_datas()
    print(update_db(datas))
