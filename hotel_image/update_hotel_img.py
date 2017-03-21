import pymysql

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
    conn = pymysql.connect(db='update_img', **SQL_DICT)
    online_conn = pymysql.connect(db='onlinedb', **SQL_DICT)
    _count = 0
    data = []
    with online_conn as online_cursor:
        with conn as cursor:
            for uid in uid_set:
                img_list = []
                online_cursor.execute('select source,sid from hotel_unid where uid=%s', (uid,))
                for line in online_cursor.fetchall():
                    cursor.execute('select hotel_jpg from cloud_pic where source=%s and source_id=%s', line)
                    for line in cursor.fetchall():
                        img_list.extend(line[0].split('|'))
                try:
                    cursor.execute('select pic_md5 from pic_relation where pic_md5 in ({0}) group by file_md5'.format(
                        ','.join(map(lambda x: '"' + x + '"', img_list))))
                except:
                    continue
                img_result = []
                for line in cursor.fetchall():
                    img_result.append(line[0])
                data.append(('|'.join(img_result), img_result[0], uid))
                _count += 1
                if _count % 100 == 0:
                    online_cursor.executemany('update hotel set img_list=%s, first_img=%s where uid=%s', data)
                    data = []
                    print(_count)
        online_cursor.executemany('update hotel set img_list=%s, first_img=%s where uid=%s', data)
    conn.close()
    online_conn.close()
