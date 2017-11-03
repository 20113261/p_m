# coding=utf-8
import toolbox.Common
from pymysql.cursors import DictCursor
from my_lib.get_similar_word import get_similar_word
from service_platform_conn_pool import poi_ori_pool, base_data_pool, spider_data_base_data_pool


def get_shop_info(uid):
    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT name, name_en FROM chat_shopping WHERE id=%s;''', (uid,))
    _res_1 = cursor.fetchone()
    cursor.close()
    conn.close()

    conn = base_data_pool.connection()
    cursor = conn.cursor()
    _res = cursor.execute('''SELECT
  name,
  name_en
FROM chat_shopping
WHERE tag_id = 9 AND id=%s;''', (uid,))
    if _res:
        _res_2 = cursor.fetchone()
    else:
        _res_2 = ('', '')
    cursor.close()
    conn.close()
    _res = list()
    _res.extend(_res_1)
    _res.extend(_res_2)
    return _res


def chinese_percent(string):
    if not toolbox.Common.is_legal(string):
        return False
    return len(list(filter(lambda x: toolbox.Common.is_chinese(x), string))) / len(string)


def latin_percent(string):
    if not toolbox.Common.is_legal(string):
        return False
    return len(list(filter(lambda x: toolbox.Common.is_latin_and_punctuation(x), string))) / len(string)


priority = {
    'online': 20,
    'qyer': 15,
    'daodao': 10,
}


def get_name(names):
    r = sorted(names, key=lambda x: (
        # 先合法
        toolbox.Common.is_legal(x[0]),
        # 中文多
        chinese_percent(x[0]),
        # 拉丁文多
        latin_percent(x[0]),
        # 源
        priority[x[1]]
    ),
               reverse=True)
    print(r)
    return r[0][0]


def update_outlets(uid, name, cid):
    conn = spider_data_base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''UPDATE chat_shopping
SET name = %s, tag_id = 9, city_id = %s
WHERE id = %s;''', (name, cid, uid))
    cursor.close()
    conn.close()

    conn = poi_ori_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''UPDATE chat_shopping
SET name = %s, norm_tagid = '奥特莱斯', norm_tagid_en = 'Outlet', city_id = %s
WHERE id = %s;''', (name, cid, uid))
    cursor.close()
    conn.close()


def task():
    name_dict = {}
    en_dict = {}
    query_sql = '''SELECT
  id,
  name,
  name_en
FROM chat_shopping;'''
    conn = poi_ori_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(query_sql)

    for line in cursor.fetchall():
        miaoji_id = line['id']
        name = line['name']
        name_en = line['name_en']
        name_dict[name] = miaoji_id
        en_dict[get_similar_word(name_en)] = miaoji_id

    cursor.close()
    conn.close()

    rows = []
    query_sql = '''SELECT *
FROM qyer_outlets_new
WHERE city_id IS NOT NULL AND city_id != 'NULL';'''
    conn = poi_ori_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(query_sql)
    for line in cursor.fetchall():
        name = line['name']
        name_en = get_similar_word(line['name_en'])
        if (name in name_dict) and line['name'] != '':
            uid = name_dict.get(name, '')
            u_name, u_name_en, o_name, o_name_en = get_shop_info(uid)
            final_name = get_name(
                [(line['name'], 'qyer'), (line['name_en'], 'qyer'), (u_name, 'daodao'), (u_name_en, 'daodao'),
                 (o_name, 'online'), (o_name_en, 'online')])
            update_outlets(cid=line['city_id'], name=final_name, uid=uid)
            print(uid, final_name, line['city_id'])
        elif (name_en in en_dict) and line['name_en'] != '':
            uid = en_dict.get(name_en, '')
            u_name, u_name_en, o_name, o_name_en = get_shop_info(uid)
            final_name = get_name(
                [(line['name'], 'qyer'), (line['name_en'], 'qyer'), (u_name, 'daodao'), (u_name_en, 'daodao'),
                 (o_name, 'online'), (o_name_en, 'online')])
            update_outlets(cid=line['city_id'], name=final_name, uid=uid)
            print(uid, final_name, line['city_id'])
        else:
            continue
    cursor.close()
    conn.close()

    import csv
    f = open('/tmp/outlets.csv', 'w', encoding='utf8')
    writer = csv.writer(f)
    writer.writerow(['qyer_id', '名称', '英文名', 'city_id', '匹配条件', '匹配项', 'ID'])
    for i in set(rows):
        print(i)
        writer.writerow(i)


if __name__ == '__main__':
    task()
