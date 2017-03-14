# coding=utf-8
import json
import pymysql
from collections import defaultdict
from mysql_config import attr_merge
from pymysql.cursors import DictCursor

# import db_localhost as db

INFO_LIST = ['name', 'name_en', 'map_info', 'address', 'star', 'plantocounts', 'beentocounts', 'ranking', 'grade',
             'commentcounts', 'tagid', 'url', 'site', 'phone', 'introduction', 'opentime', 'recommend_lv',
             'recommended_time', 'prize', 'traveler_choice', 'imgurl']


def get_qyer_id():
    qyer_id_set = set()
    conn = pymysql.connect(**attr_merge)
    with conn as cursor:
        cursor.execute('select source_id from attr_merge.attr_unid where source="qyer"')
        for line in cursor.fetchall():
            qyer_id_set.add(line[0])
    conn.close()
    return qyer_id_set


def get_tp_id():
    tp_id_set = set()
    conn = pymysql.connect(**attr_merge)
    with conn as cursor:
        cursor.execute('select source_id from attr_merge.attr_unid where source="daodao"')
        for line in cursor.fetchall():
            tp_id_set.add(line[0])
    conn.close()
    return tp_id_set


def get_other_id():
    other_id_set = set()
    conn = pymysql.connect(**attr_merge)
    with conn as cursor:
        cursor.execute('select source_id from attr_merge.attr_unid where source!="daodao" and source!="qyer"')
        for line in cursor.fetchall():
            other_id_set.add(line[0])
    conn.close()
    return other_id_set


def get_qyer_info(qyer_source_id_set):
    qyer_info_dict = {}
    conn = pymysql.connect(**attr_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from attr where source="qyer" AND id in (%s)' % ",".join(
            ["\"" + x + "\"" for x in qyer_source_id_set])
        cursor.execute(sql)
        for line in cursor.fetchall():
            qyer_info_dict[line['id']] = line
    conn.close()
    return qyer_info_dict


def get_tp_info(tp_source_id_list):
    tp_info_dict = {}
    conn = pymysql.connect(**attr_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from attr where source="daodao" and id in (%s)' % ",".join(
            ["\"" + x + "\"" for x in tp_source_id_list])
        cursor.execute(sql)
        for line in cursor.fetchall():
            tp_info_dict[line['id']] = line
    conn.close()
    return tp_info_dict


def get_other_info(other_id_list):
    other_info_dict = {}
    conn = pymysql.connect(**attr_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from attr where source!="qyer" and source!="daodao" and id in (%s)' % ",".join(
            ["\"" + x + "\"" for x in other_id_list])
        cursor.execute(sql)
        for line in cursor.fetchall():
            other_info_dict[line['id']] = line
    conn.close()
    return other_info_dict


def get_attr_info(city_id):
    _dict = {}
    conn = pymysql.connect(**attr_merge)
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = 'select * from attr where (id, source) in (select source_id, source from attr_unid where city_id=%s)'
        cursor.execute(sql, city_id)
        for line in cursor.fetchall():
            _dict[(line['id'], line['source'])] = line
    conn.close()
    return _dict


def data_merge(attr_infos, miaoji_id, city_id, city_name, country):
    conn = pymysql.connect(**attr_merge)
    with conn as cursor:
        source_set = set()
        data_dict = {}
        for info_name in INFO_LIST:
            data_dict[info_name] = {}
        for attr_info in attr_infos:
            source = attr_info['source']
            source_set.add(source)
            for info_name in INFO_LIST:
                val = (attr_info.get(info_name, '') or '')
                if val == 'NULL':
                    data_dict[info_name][source] = ''
                elif info_name != 'phone':
                    data_dict[info_name][source] = val
                else:
                    data_dict[info_name][source] = val.replace('电话号码：', '').strip()
        for k in data_dict:
            data_dict[k] = json.dumps(data_dict[k])

        sql = 'insert ignore into attr_merge.attraction_middle(`id`,`name`,`name_en`,`data_source`,`city_id`,`city`,`map_info`,`address`,`star`,`plantocount`,`beentocount`,`real_ranking`,`grade`,`commentcount`,`tagid`,`url`,`website_url`,`phone`,`introduction`,`open_desc`,`recommend_lv`,`visit_time`,`prize`,`traveler_choice`,`image_list`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        data = (
            miaoji_id, data_dict['name'], data_dict['name_en'], '|'.join(source_set), city_id, city_name,
            data_dict['map_info'], data_dict['address'],
            data_dict['star'], data_dict['plantocounts'], data_dict['beentocounts'],
            data_dict['ranking'], data_dict['grade'],
            data_dict['commentcounts'],
            data_dict['tagid'], data_dict['url'], data_dict['site'], data_dict['phone'], data_dict['introduction'],
            data_dict['opentime'], data_dict['recommend_lv'], data_dict['recommended_time'], data_dict['prize'],
            data_dict['traveler_choice'],
            data_dict['imgurl'])
        res = cursor.execute(sql, data)
    conn.close()
    return res


def task():
    task_dict = defaultdict(list)
    conn = pymysql.connect(**attr_merge)
    with conn as cursor:
        sql = 'select id,source,source_id,city_id,city_name,country_name from attr_merge.attr_unid'
        cursor.execute(sql)
        for line in cursor.fetchall():
            miaoji_id = line[0]
            task_dict[miaoji_id].append(
                (line[1], line[2], line[3], line[4], line[5]))
    conn.close()
    return task_dict


def new_task():
    task_dict = defaultdict(defaultdict(dict))
    conn = pymysql.connect(**attr_merge)
    with conn as cursor:
        sql = 'select id,source,source_id,city_id,city_name,country_name from attr_merge.attr_unid order by city_id, id'
        cursor.execute(sql)
        for line in cursor.fetchall():
            if line[0] not in task_dict[(line[3], line[4], line[5])]:
                task_dict[(line[3], line[4], line[5])] = []
            task_dict[(line[3], line[4], line[5])].append(line[1], line[2])
    conn.close()
    return task_dict


if __name__ == '__main__':
    qyer_info_dict = get_qyer_info(get_qyer_id())
    tp_info_dict = get_tp_info(get_tp_id())
    other_info_dict = get_other_info(get_other_id())
    task_dict = task()
    datas = []
    other_source_set = {'baidu', 'mafengwo'}
    for key, values in task_dict.items():
        city_id = values[0][2]
        city_name = values[0][3]
        country_name = values[0][4]
        attr_infos = []
        for value in values:
            if value[0] == 'qyer':
                attr_infos.append(qyer_info_dict[value[1]])
            elif value[0] == 'daodao':
                attr_infos.append(tp_info_dict[value[1]])
            elif value[0] in other_source_set:
                attr_infos.append(other_info_dict[value[1]])
        data_merge(attr_infos, key, city_id, city_name, country_name)

        # todo merge new way
        # task_dict = task()
        # datas = []
        # other_source_set = {'baidu', 'mafengwo'}
        # for key, values in task_dict.items():
        #     city_id = values[0][2]
        #     city_name = values[0][3]
        #     country_name = values[0][4]
        #     attr_info_dict = get_attr_info(city_id=city_id)
        #     attr_infos = []
        #     for value in values:
        #         if value[0] == 'qyer':
        #             attr_infos.append(qyer_info_dict[value[1]])
        #         elif value[0] == 'daodao':
        #             attr_infos.append(tp_info_dict[value[1]])
        #         elif value[0] in other_source_set:
        #             attr_infos.append(other_info_dict[value[1]])
        #     data_merge(attr_infos, key, city_id, city_name, country_name)

        # print(len(get_attr_info('10001')))
