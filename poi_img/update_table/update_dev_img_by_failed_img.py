import pymysql

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
    __count = 0
    id_img_dict = {}
    if s_type == 'attr':
        sql = 'select file_name, sid, pic_size from attr_bucket_relation where `use`=0'
    elif s_type == 'rest':
        sql = 'select file_name, sid, pic_size from rest_bucket_relation where `use`=0'
    elif s_type == 'shop':
        sql = 'select file_name, sid, pic_size from shop_bucket_relation where `use`=0'
    else:
        raise TypeError()
    conn = pymysql.connect(**__img_sql_dict)
    with conn as cursor:
        cursor.execute(sql)
        for file_name, sid, size in cursor.fetchall():
            __count += 1
            if __count % 10000 == 0:
                print('Prepare', __count)
            try:
                h, w = eval(size)
            except:
                continue
            __proportion = float(w) / float(h)
            if __proportion > 0.9:
                continue
            if sid not in id_img_dict:
                id_img_dict[sid] = (file_name, __proportion)
            elif id_img_dict[sid][1] < __proportion:
                id_img_dict[sid] = (file_name, __proportion)
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
    conn = pymysql.connect(db=db, **__merge_dict)
    with conn as cursor:
        cursor.execute(sql)
        for line in cursor.fetchall():
            yield line[0]
    conn.close()


def update_db(args, s_type):
    if s_type == 'attr':
        sql = 'update ignore {} set first_image=%s,image_list=%s where id=%s'.format(ATTRACTION_TABLE)
    elif s_type == 'rest':
        sql = 'update ignore {} set first_image=%s,image_list=%s where id=%s'.format(RESTAURANT_TABLE)
    elif s_type == 'shop':
        sql = 'update ignore {} set first_image=%s,image_list=%s where id=%s'.format(SHOPPING_TABLE)
    else:
        raise TypeError()
    conn = pymysql.connect(db=db, **__merge_dict)
    with conn as cursor:
        res = cursor.executemany(sql, args)
    conn.close()
    return res


if __name__ == '__main__':
    # -------- Variables ----------

    S_TYPE = 'attr'
    ATTRACTION_TABLE = 'chat_attraction_new'
    RESTAURANT_TABLE = 'chat_restaurant_new'
    SHOPPING_TABLE = 'chat_shopping_new'

    # -----------------------------
    if S_TYPE == 'attr':
        BUCKET_TABLE = 'attr_bucket_relation'
        db = 'attr_merge'
    elif S_TYPE == 'rest':
        BUCKET_TABLE = 'rest_bucket_relation'
        db = 'rest_merge'
    elif S_TYPE == 'shop':
        BUCKET_TABLE = 'shop_bucket_relation'
        db = 'shop_merge'
    else:
        raise TypeError("Error Key Type " + S_TYPE)
    conn = pymysql.connect(host='10.10.189.213', user='hourong', passwd='hourong', db='update_img', charset="utf8")
    id_img = get_id_img(S_TYPE)
    datas = []
    count = 0
    with conn as cursor:
        for mid in get_task(S_TYPE):
            if mid in id_img:
                file_name, _ = id_img[mid]
                datas.append((file_name, file_name, mid))
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
