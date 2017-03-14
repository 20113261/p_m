import json

import db_localhost as db

'''
insert into attr_merge.attr_unid (`id`,`name`,`name_en`,`source`,`source_id`,`site`,`city_id`,`city_name`,`country_name`) select id,name,name_en,source,source_id,site,city_id,city_name,country_name from hourong.attr_unid where source='daodao' or source='qyer'
'''
def get_qyer(qyer_list):
    sql = 'select id,name,name_en,site from hourong.qyer where id in (%s)' % ','.join(
        ["\"" + x + "\"" for x in qyer_list])
    source_id_info = {}
    for line in db.QueryBySQL(sql):
        source_id = line['id']
        name = line['name']
        name_en = line['name_en']
        site = line['site']
        source_id_info[source_id] = (name, name_en, site)
    return source_id_info


def get_daodao(daodao_list):
    sql = 'select id,name,name_en,site from hourong.tp_attr_basic_0801 where id in (%s)' % ','.join(
        ["\"" + x + "\"" for x in daodao_list])
    source_id_info = {}
    for line in db.QueryBySQL(sql):
        source_id = line['id']
        name = line['name']
        name_en = line['name_en']
        site = line['site']
        source_id_info[source_id] = (name, name_en, site)
    return source_id_info


def insert_db(args):
    sql = 'insert ignore into attr_merge.attr_unid(`id`,`name`,`name_en`,`source`,`source_id`,`site`,`city_id`,`city_name`,`country_name`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    return db.ExecuteSQLs(sql, args)


def task():
    qyer_list = []
    daodao_list = []
    for line in db.QueryBySQL('select source_id from hourong.attr_unid where source="qyer|daodao"'):
        source_id = json.loads(line['source_id'])
        daodao_id = source_id['daodao']
        qyer_id = source_id['qyer']
        qyer_list.append(qyer_id)
        daodao_list.append(daodao_id)
    qyer_info = get_qyer(qyer_list)
    daodao_info = get_daodao(daodao_list)
    datas = []
    for line in db.QueryBySQL(
            'select id,source_id,city_id,city_name,country_name from hourong.attr_unid where source="qyer|daodao"'):
        miaoji_id = line['id']
        source_id = json.loads(line['source_id'])
        daodao_id = source_id['daodao']
        name, name_en, site = daodao_info.get(daodao_id)
        qyer_id = source_id['qyer']
        q_name, q_name_en, q_site = qyer_info.get(qyer_id)
        city_id = line['city_id']
        city_name = line['city_name']
        country_name = line['country_name']
        data = (miaoji_id, name, name_en, 'daodao', daodao_id, site, city_id, city_name, country_name)
        datas.append(data)
        data = (miaoji_id, q_name, q_name_en, 'qyer', qyer_id, q_site, city_id, city_name, country_name)
        datas.append(data)
    print(insert_db(datas))


if __name__ == '__main__':
    task()
