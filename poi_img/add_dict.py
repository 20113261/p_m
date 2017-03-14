import hashlib
import re
from collections import defaultdict

import db_img
import db_localhost
import redis

redis_dict = redis.Redis(host='10.10.180.145', port=6379, db=10)


def get_img_id_count(s_type):
    count_dict = defaultdict(list)
    if s_type == 'attr':
        sql = 'select file_name from attr_bucket_relation'
    elif s_type == 'rest':
        sql = 'select file_name from rest_bucket_relation'
    elif s_type == 'shop':
        sql = 'select file_name from shop_bucket_relation'
    else:
        raise Exception("Error Type")
    for line in db_img.QueryBySQL(sql):
        mid, count = line['file_name'].split('.jpg')[0].split('_')
        count_dict[mid].append(int(count))
    for k, v in list(count_dict.items()):
        redis_dict.set(k, max(v))
    print((len(count_dict)))


def get_img_flag():
    sql = 'select * from image_info'
    count = 0
    for line in db_localhost.QueryBySQL(sql):
        redis_dict.set(line['file_name'], line['file_info'])
        count += 1
    print(count)


if __name__ == '__main__':
    # -------- Variables ----------

    S_TYPE = 'attr'
    file_path = '/tmp/img_url_1119'

    # -----------------------------
    pattern = re.compile('-d(\d+)')
    get_img_id_count(S_TYPE)
    get_img_flag()
    count = 0
    for url in open(file_path):
        redis_dict.set(hashlib.md5(url.strip()).hexdigest() + '.jpg', url.strip())
        count += 1
    print(count)
