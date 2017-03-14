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
        try:
            count_dict[mid].append(int(count))
        except:
            continue
    for k, v in list(count_dict.items()):
        redis_dict.set(k, max(v))
    print((len(count_dict)))


if __name__ == '__main__':
    # -------- Variables ----------

    S_TYPE = 'attr'

    # -----------------------------
    get_img_id_count(S_TYPE)
