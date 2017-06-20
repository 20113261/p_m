import pymysql
import time

SQL_DICT = dict(host='10.10.189.213', user='hourong', passwd='hourong', charset="utf8")


def get_uid_set():
    _set = set()
    conn = pymysql.connect(db='onlinedb', **SQL_DICT)
    with conn as cursor:
        sql = 'select distinct uid from hotel'
        cursor.execute(sql)
        for line in cursor.fetchall():
            _set.add(line[0])
    conn.close()
    print('uid fetch OK')
    return _set


if __name__ == '__main__':
    uid_set = get_uid_set()
    # uid_set = {'ht20019880'}
    conn = pymysql.connect(db='update_img', **SQL_DICT)
    online_conn = pymysql.connect(db='onlinedb', **SQL_DICT)
    _count = 0
    data = []
    for uid in uid_set:
        online_cursor = online_conn.cursor()
        cursor = conn.cursor()
        online_cursor.execute('''SELECT
  source,
  sid
FROM hotel_unid
WHERE uid = %s;''', (uid,))

        start = time.time()
        img_result = []
        for each in online_cursor.fetchall():
            cursor.execute('''SELECT pic_md5
FROM pic_relation
WHERE source = %s AND source_id = %s GROUP BY file_md5;''', each)

            for line in cursor.fetchall():
                img_result.append(line[0])
        if img_result:
            data.append(('|'.join(img_result), img_result[0], uid))
        cursor.close()
        online_cursor.close()
        _count += 1
        if _count % 100 == 0:
            online_cursor = online_conn.cursor()
            online_cursor.executemany('update hotel set img_list=%s, first_img=%s where uid=%s', data)
            online_cursor.close()
            data = []
            print(_count)

    online_cursor = online_conn.cursor()
    online_cursor.executemany('update hotel set img_list=%s, first_img=%s where uid=%s', data)
    online_cursor.close()
    data = []
    print(_count)
