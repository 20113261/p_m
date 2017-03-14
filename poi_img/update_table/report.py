# coding=utf-8
from . import database
from . import db_city


def get_city_id():
    id_set = set()
    sql = 'select id from city where country="日本" or country="美国"'
    for line in db_city.QueryBySQL(sql):
        id_set.add(line['id'])
    return id_set


def get_task(s_type, is_new, is_have):
    if s_type == 'attr':
        t_type = 'attraction'
    elif s_type == 'rest':
        t_type = 'restaurant'
    elif s_type == 'shop':
        t_type = 'shopping'

    sql = 'select city_id from chat_{0}{1} where image_list{2}=""'.format(t_type, '_new' if is_new else '',
                                                                          '!' if is_have else '')

    for line in database.QueryBySQL(sql):
        yield line['city_id']


if __name__ == '__main__':
    city_id_set = get_city_id()
    count = 0
    for city_id in get_task('shop', True, True):
        if city_id in city_id_set:
            count += 1
    print(count)
