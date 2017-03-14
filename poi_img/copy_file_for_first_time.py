# coding=utf-8
import hashlib
import re
import os
import pymysql
from collections import defaultdict
from shutil import move

# import db_img
# import db_localhost
import redis

redis_dict = redis.Redis(host='10.10.180.145', port=6379, db=10)


def get_md5(src):
    return hashlib.md5(src.encode()).hexdigest()


def file_md5(f_name):
    hash_md5 = hashlib.md5()
    with open(f_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_mid_image_list_dict(s_type):
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='hourong')
    _res = defaultdict(list)
    if s_type == 'attr':
        sql = 'select id, image from data_prepare.attraction_tmp'
    elif s_type == 'rest':
        sql = 'select id, image_urls from data_prepare.restaurant_tmp'
    elif s_type == 'shop':
        sql = 'select id, image from data_prepare.shopping_tmp'
    else:
        raise TypeError()

    with conn as cursor:
        cursor.execute(sql)
        for line in cursor.fetchall():
            for url in line[1].split('|'):
                _res[line[0]].append(get_md5(url) + '.jpg')
    conn.close()
    return _res


if __name__ == '__main__':
    # 需要修改的变量
    S_TYPE = 'attr'
    src_path = '/search/image/img_url_1119_celery/'
    dst_path = '/search/image/attr_result_1119/'

    # -----------------------------------------------

    RELATION_TABLE = '{}_bucket_relation'.format(S_TYPE)
    BUCKET_NAME = 'mioji-' + S_TYPE
    # Prepare
    pattern = re.compile('-d(\d+)')
    count = 0
    source_id_set = set()
    mid_image_list_dict = get_mid_image_list_dict(S_TYPE)
    conn = pymysql.connect(host='10.10.189.213', user='hourong', passwd='hourong', charset='utf8', db='update_img')
    cursor = conn.cursor()

    # Run
    count = 0
    _suc_count = 0
    for mid, img_list in list(mid_image_list_dict.items()):
        for img_name in img_list:
            img_count = redis_dict.get(mid)
            if not img_count:
                new_name = mid + '_1.jpg'
                _img_now_count = "1"
            else:
                new_count = str(int(img_count) + 1)
                _img_now_count = new_count
                new_name = mid + "_" + new_count + '.jpg'
            try:
                flag, y, x = redis_dict.get(img_name + "_flag").split('###')
            except:
                continue
            size = str((y, x))
            img_url = redis_dict.get(img_name)
            if flag == '0':
                count += 1
                # (RELATION_TABLE, file_name, miaoji_id, url, BUCKET_NAME, size, md5_name, pic_md5, 'machine', '1','online')
                _file_md5 = file_md5(os.path.join(src_path, img_name))
                data = (new_name, new_name.split('_', 1)[0], img_url, BUCKET_NAME, size,
                        img_name.replace('.jpg', ''),
                        _file_md5,
                        'machine', '1', 'online')
                move(os.path.join(src_path, img_name), os.path.join(dst_path, new_name))
                try:
                    res = cursor.execute(
                        'insert into {0} (file_name,sid,url,bucket_name,pic_size,url_md5,pic_md5,source,`use`,status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(
                            RELATION_TABLE), data)
                    redis_dict.set(mid, _img_now_count)
                    _suc_count += res or 0
                except Exception as exc:
                    print(str(exc))
                if count % 500 == 0:
                    print(data)
                    print(_suc_count)
                    _suc_count = 0
    print(count)
    conn.close()
