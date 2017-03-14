# coding=utf-8
import db_localhost as db
import db_114_35_shop
import db_test
import db_online

from my_lib.get_similar_word import get_similar_word
from my_lib.url_is_similar import get_modify_url


def task():
    name_dict = {}
    en_dict = {}
    site_dict = {}
    sql = 'select id,name,name_en,website_url from chat_shopping'
    for line in db_114_35_shop.QueryBySQL(sql):
        miaoji_id = line['id']
        name = line['name']
        name_en = line['name_en']
        site = get_modify_url(line['website_url'])
        site_dict[site] = miaoji_id
        name_dict[name] = miaoji_id
        en_dict[get_similar_word(name_en)] = miaoji_id

    rows = []
    for line in db.QueryBySQL('select id,name,name_en,site from qyer_outlets'):
        source_id = line['id']
        name = line['name']
        name_en = get_similar_word(line['name_en'])
        site = get_modify_url(line['site'])
        if (name in name_dict) and line['name'] != '':
            rows.append((source_id, line['name'], line['name_en'], line['site'], 'name', name, name_dict.get(name, '')))
        elif (name_en in en_dict) and line['name_en'] != '':
            rows.append(
                (source_id, line['name'], line['name_en'], line['site'], 'name_en', name_en, en_dict.get(name_en, '')))
        elif (site in site_dict) and line['site'] != '':
            rows.append((source_id, line['name'], line['name_en'], line['site'], 'site', site, site_dict.get(site, '')))
    import csv
    f = open('/tmp/outlets.csv', 'w')
    writer = csv.writer(f)
    writer.writerow(['qyer_id', '名称', '英文名', '官网', '匹配条件', '匹配项', 'ID'])
    for i in set(rows):
        writer.writerow(i)


if __name__ == '__main__':
    task()
