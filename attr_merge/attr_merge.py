import pymysql
from pymysql.cursors import DictCursor

from Config.settings import dev_conf, attr_merge_conf
from my_lib.get_similar_word import get_similar_word

TARGET_TABLE = 'target_city_new'


def get_max_id():
    id_set = set()
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor()

    cursor.execute('''SELECT id
FROM chat_attraction;''')

    for line in cursor.fetchall():
        try:
            id_set.add(int(line[0][1:]))
        except Exception:
            continue

    cursor.execute('''SELECT id
FROM attr_unid;''')

    for line in cursor.fetchall():
        try:
            id_set.add(int(line[0][1:]))
        except Exception:
            continue

    return 'v' + str(max(id_set))


max_id = get_max_id()


def similar_dict():
    name_dict = {}
    en_dict = {}

    sql = '''SELECT
  id,
  name,
  name_en,
  city_id
FROM attr_unid
ORDER BY id;'''

    city_id_list = []

    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor()

    cursor.execute(sql)
    for line in cursor.fetchall():
        mid = line['id']
        name = line['name']
        name_en = get_similar_word(line['name_en'])
        city_id = line['city_id']
        city_id_list.append(city_id)
        name_key = city_id + '|_|_|' + name
        en_key = city_id + '|_|_|' + name_en
        if name_key not in name_dict:
            name_dict[name_key] = mid
        if en_key not in en_dict:
            en_dict[en_key] = mid

    return name_dict, en_dict, city_id_list


def get_task_city():
    conn = pymysql.connect(**dev_conf)
    cursor = conn.cursor(cursor=DictCursor)
    sql = """SELECT
  id           AS city_id,
  city.name    AS city_name,
  city.name_en AS city_name_en,
  country.name AS country_name,
  map_info
FROM city
  JOIN country ON city.country_id = country.mid;"""
    cursor.execute(sql)
    yield from cursor.fetchall()


def insert_db(args):
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor()
    sql = '''INSERT INTO attr_unid (id, city_id, city_name, country_name, city_map_info, source, source_id, name, name_en, map_info, grade, star, ranking, address, url)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
    res = cursor.executemany(sql, args)
    cursor.close()
    conn.close()
    return res


def get_new_miaoji_id():
    global max_id
    max_id = 'v' + str(int(max_id[1:]) + 1)
    return max_id


def get_attr_info(source, cid):
    sql = '''SELECT *
FROM attr
WHERE source = %s AND city_id = %s;'''
    conn = pymysql.connect(**attr_merge_conf)
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql, (source, cid))
    yield from cursor.fetchall()


def task(task_source):
    name_dict, en_dict, city_id_list = similar_dict()

    count = 0
    data = []
    for each_city in get_task_city():
        city_id = each_city['city_id']
        city_name = each_city['city_name']
        city_name_en = each_city['city_name_en']
        city_map_info = each_city['map_info']
        country_name = each_city['country_name']

        attr_info = get_attr_info(task_source, city_id)

        for each_attr_info in attr_info:
            source = task_source
            source_id = each_attr_info['id']
            name = each_attr_info['name']
            map_info = each_attr_info['map_info']
            name_en = get_similar_word(each_attr_info['name_en'] or '')
            name_key = city_id + '|_|_|' + (name or '')
            name_en_key = city_id + '|_|_|' + (name_en or '')
            if (name_key in name_dict or name_key in en_dict) and (each_attr_info['name'] != '') and (
                        each_attr_info['name'] is not None):
                miaoji_id = name_dict.get(name_key, '')
            elif (name_en_key in en_dict or name_en_key in name_dict) and (each_attr_info['name_en'] != '') and (
                        each_attr_info['name_en'] is not None):
                miaoji_id = en_dict.get(name_en_key, '')
            else:
                miaoji_id = get_new_miaoji_id()
            count += 1

            # id, city_id, city_name, country_name, city_map_info, source, source_id, name, name_en, map_info,
            # grade, star, ranking, address, url
            each_data = (
                miaoji_id, city_id, city_name, country_name, city_map_info, source, source_id, each_attr_info['name'],
                each_attr_info['name_en'], each_attr_info['map_info'], each_attr_info['grade'], each_attr_info['star'],
                each_attr_info['ranking'], each_attr_info['address'], each_attr_info['url']
            )

            # 增加进一步融合的 key
            name_dict[name_key] = miaoji_id
            en_dict[name_en_key] = miaoji_id

            # 添加数据
            data.append(each_data)

            if count % 1000 == 0:
                insert_db(data)
                data = []
                print(count)
    print(insert_db(data))
    print(count)


if __name__ == '__main__':
    task('daodao')
