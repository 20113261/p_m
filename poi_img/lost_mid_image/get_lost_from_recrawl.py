# coding=utf-8
import json
import os
from collections import defaultdict

import db_localhost

from . import database


def __get_task_attr_shop(s_type):
    # attraction, shopping
    attr_sql = 'select id,url from chat_attraction where image_list=""'
    shop_sql = 'select id,url from chat_shopping where image_list=""'
    if s_type == 'attr':
        sql = attr_sql
    elif s_type == 'shop':
        sql = shop_sql
    else:
        raise Exception("Type Error")
    error_list = []
    count = 0
    for line in database.QueryBySQL(sql):
        l_json = json.loads(line['url'])
        if 'daodao' in l_json:
            url = l_json['daodao']
            if 'Attraction_Review' in url:
                count += 1
                yield url, line['id']
            else:
                error_list.append(line['id'])
        else:
            error_list.append(line['id'])
    print(count)


def __get_task_rest():
    # restaurant
    sql = 'select id,res_url from chat_restaurant where image_list=""'
    error_list = []
    count = 0
    for line in database.QueryBySQL(sql):
        url = line['res_url']
        if 'Restaurant_Review' in url:
            count += 1
            yield url, line['id']
        else:
            error_list.append(line['id'])
    print(count)


def get_task(s_type):
    if s_type in ['attr', 'shop']:
        gen = __get_task_attr_shop(s_type)
    elif s_type == 'rest':
        gen = __get_task_rest()
    else:
        raise Exception('Type Error')

    for res in gen:
        yield res


def get_task_dict():
    id_img_list_dict = defaultdict(list)
    sql = 'select mid,img_list from image_recrawl'
    for line in db_localhost.QueryBySQL(sql):
        id_img_list_dict[line['mid']].extend(line['img_list'].split('|'))
    return id_img_list_dict


def get_mid_img_list_dict(path, s_type):
    poi_set = set()
    mid_img_list_dict = defaultdict(list)
    for file_name in os.listdir(path):
        poi_id = file_name.split('_')[0]
        poi_set.add(poi_id)

    id_img_list_dict = get_task_dict()
    for url, mid in get_task(s_type):
        if (mid not in poi_set) and (mid in id_img_list_dict):
            mid_img_list_dict[mid].extend(id_img_list_dict[mid])
    return id_img_list_dict


if __name__ == '__main__':
    # 变量 --------------------------

    path = '/search/image/rest_result/'
    S_TYPE = 'rest'

    # -------------------------------
    d = get_mid_img_list_dict(path, S_TYPE)
    print('Hello World')
    # poi_set = set()
    # for file_name in os.listdir(path):
    #     poi_id = file_name.split('_')[0]
    #     poi_set.add(poi_id)
    #
    # id_img_list_dict = get_task_dict()
    # count = 0
    # count_2 = 0
    # for url, mid in get_task(S_TYPE):
    #     if mid not in poi_set and mid in id_img_list_dict:
    #         # print mid, id_img_list_dict[mid]
    #         count += 1
    #
    # print count
