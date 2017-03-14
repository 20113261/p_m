import pymysql

if __name__ == '__main__':
    conn = pymysql.connect(host='10.10.189.213', user='hourong', passwd='hourong', charset='utf8', db='onlinedb')
    _count = 0
    _n_count = 0
    with conn as cursor:
        cursor.execute('select img_list from hotel')
        for line in cursor.fetchall():
            _count += len(line[0].split('|'))
            if _count // 10000 > _n_count:
                _n_count += 1
                print(_count)
