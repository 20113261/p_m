# import database
# import db_114_35_attr
# import db_114_35_rest
# import db_114_35_shop
# import db_img
import pymysql
import copy
from pymysql.cursors import DictCursor

__img_sql_dict = {
    'host': '10.10.189.213',
    'user': 'hourong',
    'passwd': 'hourong',
    'charset': 'utf8',
    'db': 'update_img'
}

__merge_dict = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'passwd': 'hourong',
    'charset': 'utf8'
}


def get_id_img(s_type):
    id_img_dict = {}
    if s_type == 'attr':
        sql = 'select sid,group_concat(file_name ORDER BY file_name SEPARATOR "|") as img from attr_bucket_relation where `use`=1 group by sid'
    elif s_type == 'rest':
        sql = 'select sid,group_concat(file_name ORDER BY file_name SEPARATOR "|") as img from rest_bucket_relation where `use`=1 group by sid'
    elif s_type == 'shop':
        sql = 'select sid,group_concat(file_name ORDER BY file_name SEPARATOR "|") as img from shop_bucket_relation where `use`=1 group by sid'
    else:
        raise TypeError()
    conn = pymysql.connect(**__img_sql_dict)
    with conn.cursor(cursor=DictCursor) as cursor:
        cursor.execute(sql)
        for line in cursor.fetchall():
            id_img_dict[line['sid']] = line['img']
    conn.close()
    return id_img_dict


def get_task(s_type, is_empty=True):
    if s_type == 'attr':
        sql = 'select id from {}'.format(ATTRACTION_TABLE)
    elif s_type == 'rest':
        sql = 'select id from {}'.format(RESTAURANT_TABLE)
    elif s_type == 'shop':
        sql = 'select id from {}'.format(SHOPPING_TABLE)
    else:
        raise TypeError()
    if is_empty:
        sql += ' where image_list=""'
    conn = pymysql.connect(db=db_name, **__merge_dict)
    with conn as cursor:
        cursor.execute(sql)
        for line in cursor.fetchall():
            yield line[0]
    conn.close()


def update_db(args, s_type):
    if s_type == 'attr':
        sql = 'update ignore {} set first_image=%s,image_list=%s where id=%s'.format(ATTRACTION_TABLE)
        db_name = 'attr_merge'
    elif s_type == 'rest':
        sql = 'update ignore {} set first_image=%s,image_list=%s where id=%s'.format(RESTAURANT_TABLE)
        db_name = 'rest_merge'
    elif s_type == 'shop':
        sql = 'update ignore {} set first_image=%s,image_list=%s where id=%s'.format(SHOPPING_TABLE)
        db_name = 'shop_merge'
    else:
        raise TypeError()
    conn = pymysql.connect(db=db_name, **__merge_dict)
    with conn as cursor:
        res = cursor.executemany(sql, args)
    conn.close()
    return res


if __name__ == '__main__':
    # -------- Variables ----------

    S_TYPE = 'rest'
    ATTRACTION_TABLE = 'chat_attraction_new'
    RESTAURANT_TABLE = 'chat_restaurant_new'
    SHOPPING_TABLE = 'chat_shopping_new'

    # -----------------------------
    if S_TYPE == 'attr':
        BUCKET_TABLE = 'attr_bucket_relation'
        db_name = 'attr_merge'
    elif S_TYPE == 'rest':
        BUCKET_TABLE = 'rest_bucket_relation'
        db_name = 'rest_merge'
    elif S_TYPE == 'shop':
        BUCKET_TABLE = 'shop_bucket_relation'
        db_name = 'shop_merge'
    else:
        raise TypeError("Error Key Type " + S_TYPE)
    conn = pymysql.connect(host='10.10.189.213', user='hourong', passwd='hourong', db='update_img', charset="utf8")
    id_img = get_id_img(S_TYPE)
    datas = []
    count = 0
    with conn as cursor:
        for mid in get_task(S_TYPE, False):
            if mid in id_img:
                img_list = id_img[mid]
                sql = 'select file_name from {} where file_name in ({}) GROUP BY pic_md5'.format(BUCKET_TABLE,
                                                                                                 ','.join(map(lambda
                                                                                                                  x: '"' + x + '"',
                                                                                                              img_list.split(
                                                                                                                  '|'))))
                cursor.execute(sql)
                final_img_list = []
                for line in cursor.fetchall():
                    final_img_list.append(line[0])
                final_img_list.sort()
                datas.append((final_img_list[0], '|'.join(final_img_list), mid))
                count += 1
                if count % 1000 == 0:
                    print(count)
            else:
                datas.append(("", "", mid))

            if count % 10000 == 0:
                print('#' * 100)
                print('Total', count)
                print('Update', update_db(datas, S_TYPE))
                datas = []

    print('#' * 100)
    print('Total', count)
    print('Update', update_db(datas, S_TYPE))
