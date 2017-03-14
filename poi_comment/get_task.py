import json
import re
from collections import defaultdict

import db_localhost

from . import database

pattern = re.compile('-d(\d+)')


def get_comment_count(s_type):
    if s_type == 'attr':
        hname = 'view'
    elif s_type in ['shop', 'rest', 'hotel']:
        hname = s_type
    else:
        raise TypeError()
    comment_count = defaultdict(int)
    sql = 'select id,count from comment_count where hname="{0}"'.format(hname)
    for line in database.QueryBySQL(sql):
        comment_count[line['id']] = int(line['count'])
    return comment_count


def get_task(s_type):
    if s_type == 'attr':
        sql = 'select id,url from chat_attraction'
    elif s_type == 'rest':
        sql = 'select id,res_url from chat_restaurant'
    elif s_type == 'shop':
        sql = 'select id,url from chat_shopping'
    else:
        raise TypeError()

    if s_type in ['attr', 'shop']:
        for line in database.QueryBySQL(sql):
            url = line['url']
            if 'Attraction_Review' in url:
                for v in list(json.loads(url).values()):
                    if 'Attraction_Review' in str(v):
                        yield line['id'], 'http://www.tripadvisor.cn/' + str(v).split('/', 3)[-1]
    if s_type == 'rest':
        for line in database.QueryBySQL(sql):
            url = line['res_url']
            if 'Restaurant_Review' in url:
                yield line['id'], 'http://www.tripadvisor.cn/' + url.split('/', 3)[-1]


def get_ready_id():
    id_set = set()
    sql = 'select distinct miaoji_id from attr_comment_1018'
    for line in db_localhost.QueryBySQL(sql):
        id_set.add(line['miaoji_id'])
    return id_set


def get_task_full(s_type):
    id_set = get_ready_id()
    comment_count = get_comment_count(s_type)
    for mid, url in get_task(s_type):
        if not comment_count[mid] and mid not in id_set:
            yield mid, url


if __name__ == '__main__':
    import random

    comment_count = get_comment_count('rest')
    count = 0
    url_set = set()
    for mid, url in get_task('rest'):
        if not comment_count[mid]:
            # print mid, url
            url_set.add(url)
            count += 1
    print(count)
    print(len(url_set))
    url_list = list(url_set)
    random.shuffle(url_list)
    for i in url_list[:10]:
        print(i)
