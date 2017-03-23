import pymysql

DEV_DICT = {
    'host': '10.10.154.38',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'devdb'
}

IMG_DICT = {
    'host': '10.10.189.213',
    'user': 'hourong',
    'password': 'hourong',
    'charset': 'utf8',
    'db': 'update_img'
}


def get_error_set():
    conn = pymysql.connect(**DEV_DICT)
    error_set = set()
    sql = 'select pic_name from PoiPictureInfomation where is_available=0'
    with conn as cursor:
        cursor.execute(sql)
        for line in cursor.fetchall():
            error_set.add((line[0].split('/')[-1],))
    conn.close()
    return error_set


def update_db(args):
    conn = pymysql.connect(**IMG_DICT)
    with conn as cursor:
        sql = 'update attr_bucket_relation set `use`=0 where file_name=%s'
        res = cursor.executemany(sql, args=args)
    conn.close()
    return res


if __name__ == '__main__':
    data = get_error_set()
    print('Total', len(data))
    print('Update', update_db(data))
