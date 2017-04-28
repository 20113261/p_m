import pymysql
import time
import hashlib

TypeDict = {
    'attr': ('attr_merge', 'chat_attraction', 'image'),
    'rest': ('rest_merge', 'chat_restaurant', 'image_urls'),
    'shop': ('shop_merge', 'chat_shopping', 'image')
}


def get_md5(src):
    return hashlib.md5(src.encode()).hexdigest()


def get_mid_set(__type, __cid_list):
    _set = set()
    if __type in TypeDict:
        db, table, key = TypeDict[__type]
    else:
        raise TypeError('Unknown type: {}'.format(__type))
    conn = pymysql.connect(host='10.10.114.35', user='hourong', passwd='hourong', charset='utf8', db=db)
    with conn as cursor:
        cursor.execute('select id from {0} where city_id in ({1})'.format(table, ','.join(
            map(lambda x: '"' + str(x) + '"', __cid_list))))
        for line in cursor.fetchall():
            _set.add(line[0])
    conn.close()
    return _set


if __name__ == '__main__':
    cid_list = []

    _type = 'rest'
    db, table, key = TypeDict[_type]

    f = open('/root/data/task/{}_img_task_{}'.format(_type, time.strftime('%y%m%d')), 'w')
    _count = 0
    conn = pymysql.connect(user='hourong', passwd='hourong', charset='utf8', db='hourong')
    conn_data_prepare = pymysql.connect(user='hourong', passwd='hourong', charset='utf8', db='data_prepare')
    conn_dev = pymysql.connect(host='10.10.114.35', user='hourong', passwd='hourong', charset='utf8', db=db)

    cursor_data_prepare = conn_data_prepare.cursor()
    cursor_dev = conn_dev.cursor()
    with conn as cursor:
        for mid in get_mid_set(_type, cid_list):
            cursor.execute('select img_list from daodao_img where mid=%s', (mid,))
            for line in cursor.fetchall():
                cursor_data_prepare.execute('update restaurant_tmp set image_urls=%s where id=%s', (line, mid))
                cursor_dev.execute('update {} set {}=%s where id=%s'.format(table, key), (line, mid))
                for img_url in line[0].split('|'):
                    print('\t'.join([mid, img_url, get_md5(img_url)]), file=f)
                    _count += 1
                    if _count % 10000 == 0:
                        print(_count)
    conn.close()
    print(_count)
