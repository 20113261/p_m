# import db_localhost as db
# import db_114_35_shop as db
# import db_114_35_attr as db
import pymysql

__sql_dict = {
    'host': '10.10.114.35',
    'user': 'hourong',
    'passwd': 'hourong',
    'charset': 'utf8',
}


def get_task():
    sql = 'select id,image_list,first_image from {0}'.format(
        TARGET_TABLE)
    conn = pymysql.connect(db=DB_NAME, **__sql_dict)
    with conn as cursor:
        cursor.execute(sql)
        for line in cursor.fetchall():
            yield line
    conn.close()


def update_db(args):
    sql = 'update {0} set first_image=%s where id=%s'.format(TARGET_TABLE)
    conn = pymysql.connect(db=DB_NAME, **__sql_dict)
    with conn as cursor:
        res = cursor.executemany(sql, args)
    conn.close()
    return res


if __name__ == '__main__':
    # -------- Variables ----------

    # TARGET_TABLE = 'data_prepare.attraction_tmp'
    # TARGET_TABLE = 'data_prepare.restaurant_tmp'
    # TARGET_TABLE = 'data_prepare.shopping_tmp'
    # TARGET_TABLE = 'chat_poi_prepare.chat_attraction'
    # TARGET_TABLE = 'data_prepare.chat_attraction'
    # TARGET_TABLE = 'data_prepare.chat_shopping'
    # TARGET_TABLE = 'data_prepare.chat_restaurant'

    TARGET_TABLE = 'chat_attraction'
    DB_NAME = 'attr_merge'
    # TARGET_TABLE = 'chat_restaurant'
    # DB_NAME = 'rest_merge'
    # TARGET_TABLE = 'chat_shopping'
    # DB_NAME = 'shop_merge'
    # -----------------------------
    data = []
    _count = 0
    for m_id, image_list, first_image in get_task():
        if (first_image not in image_list) or (first_image == ''):
            _count += 1
            if image_list and image_list != '0':
                data.append((image_list.split('|')[0], m_id))
            else:
                data.append(('', m_id))
            if _count % 10000 == 0:
                print('#' * 100)
                print('Total', _count)
                print('Update', update_db(data))
                data = []
    print('#' * 100)
    print('Total', _count)
    print('Update', update_db(data))
