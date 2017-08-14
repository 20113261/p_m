# import db_devdb
# import db_localhost as db
import pymysql
from pymysql.cursors import DictCursor
from my_lib.get_rest_info import get_tp_rest_info_by_city_id
from my_lib.get_similar_word import get_similar_word
from my_lib.url_is_similar import get_modify_url
from rest_merge.get_max_id import get_max_id

max_id = get_max_id()

TARGET_TABLE = 'target_city_new'


def similar_dict():
    name_dict = {}
    en_dict = {}
    site_dict = {}
    sql = 'select id,name,name_en,site,city_id from rest_unid'
    city_id_list = []
    _count = 0

    conn = pymysql.connect(host='10.10.180.145', user='hourong', password='hourong', charset='utf8', db='rest_merge')
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    for line in cursor.fetchall(sql):
        _count += 1
        if _count % 20000 == 0:
            print(_count)
        miaoji_id = line['id']
        name = line['name']
        name_en = get_similar_word(line['name_en'])
        site = get_modify_url(line['site'])
        city_id = line['city_id']
        city_id_list.append(city_id)
        site_dict[city_id + '|_|_|' + site] = miaoji_id
        name_dict[city_id + '|_|_|' + name] = miaoji_id
        en_dict[city_id + '|_|_|' + name_en] = miaoji_id
    return name_dict, en_dict, site_dict, city_id_list


def get_task_city():
    sql = "select id as city_id, name as city_name, name_en as city_name_en, country as country_name from city"
    conn = pymysql.connect(host='10.10.69.170', user='reader', password='miaoji1109', charset='utf8', db='base_data')
    cursor = conn.cursor()
    cursor.execute(sql)
    yield from cursor.fetchall()


def insert_db(args):
    sql = 'insert ignore into rest_merge.rest_unid (`id`,`name`,`name_en`,`source`,`source_id`,`site`,`city_id`,`city_name`,`country_name`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    return db.ExecuteSQLs(sql, args)


def get_new_miaoji_id():
    global max_id
    max_id = 'r' + str(int(max_id[1:]) + 1)
    return max_id


if __name__ == '__main__':
    name_dict, en_dict, site_dict, city_id_list = similar_dict()
    TASK_SOURCE = 'daodao'
    if TASK_SOURCE not in ['daodao', 'qyer']:
        raise Exception("Error Source")

    count = 0
    datas = []
    for each_city in get_task_city():
        city_id = each_city['city_id']
        city_name = each_city['city_name']
        city_name_en = each_city['city_name_en']
        country_name = each_city['country_name']
        # use tp source
        if TASK_SOURCE == 'daodao':
            # try:
            #     tp_source_city_id = get_tp_source_city_id((country_name, city_name, city_name_en))[0]['gid']
            # except:
            #     tp_source_city_id = ''
            # rest_infos = get_tp_rest_info(tp_source_city_id)
            rest_infos = get_tp_rest_info_by_city_id(city_id)

        for rest_info in rest_infos:
            source = TASK_SOURCE
            source_id = rest_info['id']
            name = rest_info['name'] or ''
            name_en = get_similar_word(rest_info['name_en'] or '')
            site = get_modify_url(rest_info['site'] or '')
            name_key = city_id + '|_|_|' + (name or '')
            name_en_key = city_id + '|_|_|' + (name_en or '')
            site_key = city_id + '|_|_|' + (site or '')
            if (name_key in name_dict or name_en_key in name_dict) and (rest_info['name'] != '') and (
                        rest_info['name'] is not None) and (rest_info['name'] != 'NULL'):
                miaoji_id = name_dict.get(name_key, '')
            elif (name_en_key in en_dict or name_key in en_dict) and (rest_info['name_en'] != '') and (
                        rest_info['name_en'] is not None) and (rest_info['name_en'] != 'NULL'):
                miaoji_id = en_dict.get(name_en_key, '')
            elif (site_key in site_dict) and (rest_info['site'] != '') and (rest_info['site'] is not None) and \
                    (rest_info['site'] != 'NULL'):
                miaoji_id = site_dict.get(site_key, '')
            else:
                miaoji_id = get_new_miaoji_id()
            count += 1
            data = (miaoji_id, rest_info['name'], rest_info['name_en'], source, source_id, rest_info['site'], city_id,
                    city_name, country_name)
            name_dict[name_key] = miaoji_id
            en_dict[name_en_key] = miaoji_id
            site_dict[site_key] = miaoji_id
            datas.append(data)
            if count % 1000 == 0:
                print('Insert', insert_db(datas))
                datas = []
                print('Total', count)
    print("Insert", insert_db(datas))
    print("Total", count)
