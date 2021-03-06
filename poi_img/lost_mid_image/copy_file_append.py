# coding=utf-8
import hashlib
import json
from shutil import copyfile

import db_img
import db_localhost
import redis

import poi_img.database

redis_dict = redis.Redis(host='10.10.180.145', port=6379, db=10)


def __get_task_attr_shop(s_type):
    # attraction, shopping
    attr_sql = 'select id,url from chat_attraction where image_list=""'
    shop_sql = 'select id,url from chat_shopping where image_list=""'
    if s_type == 'attr':
        sql = attr_sql
    elif s_type == 'shop':
        sql = shop_sql
    else:
        raise Exception("Type Error")
    error_list = []
    count = 0
    for line in poi_img.database.QueryBySQL(sql):
        l_json = json.loads(line['url'])
        if 'daodao' in l_json:
            url = l_json['daodao']
            if 'Attraction_Review' in url:
                count += 1
                yield url, line['id']
            else:
                error_list.append(line['id'])
        else:
            error_list.append(line['id'])
    print(count)


def __get_task_rest():
    # restaurant
    sql = 'select id,res_url from chat_restaurant where image_list=""'
    error_list = []
    count = 0
    for line in poi_img.database.QueryBySQL(sql):
        url = line['res_url']
        if 'Restaurant_Review' in url:
            count += 1
            yield url, line['id']
        else:
            error_list.append(line['id'])
    print(count)


def get_task(s_type):
    if s_type in ['attr', 'shop']:
        gen = __get_task_attr_shop(s_type)
    elif s_type == 'rest':
        gen = __get_task_rest()
    else:
        raise Exception('Type Error')

    for res in gen:
        yield res


def get_img_url(source_id_set, s_type):
    if s_type == 'attr':
        sql = 'select id,imgurl from tp_attr_basic_0801 where id in ({0})'.format(
            ','.join(["\"" + x + "\"" for x in source_id_set]))
    elif s_type == 'shop':
        sql = 'select id,imgurl from tp_shop_basic_0801 where id in ({0})'.format(
            ','.join(["\"" + x + "\"" for x in source_id_set]))
    elif s_type == 'rest':
        sql = 'select id,image_urls from tp_rest_basic_0801 where id in ({0})'.format(
            ','.join(["\"" + x + "\"" for x in source_id_set]))
    else:
        raise Exception("Error Type")
    for line in db_localhost.QueryBySQL(sql):
        if s_type in ['attr', 'shop']:
            for url in line['imgurl'].split('|'):
                if url:
                    yield url, line['id']
        elif s_type == 'rest':
            for url in line['image_urls'].split('|'):
                if url:
                    yield url, line['id']


def insert_db(args):
    sql = 'insert ignore into ' + args[
        0] + '(file_name,sid,url,bucket_name,pic_size,url_md5,pic_md5,source,`use`,status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    return db_img.ExecuteSQL(sql, args[1:])


if __name__ == '__main__':
    from .get_lost_from_recrawl import get_mid_img_list_dict

    # 需要修改的变量
    S_TYPE = 'rest'
    src_path = '/search/image/rest_url_list_celery/'
    dst_path = '/search/image/rest_result/'
    RELATION_TABLE = 'rest_bucket_relation'
    BUCKET_NAME = 'mioji-rest'

    # -----------------------------------------------

    # Prepare
    mid_img_list_dict = get_mid_img_list_dict(dst_path, S_TYPE)

    # COUNT

    # count = 0
    # for mid, source_id in mid_source_id_dict.items():
    #     img_list = result_dict[source_id]
    #     for img_name in img_list:
    #         count += 1
    # print count

    # Run

    count = 0
    for mid, raw_list in list(mid_img_list_dict.items()):
        img_list = [hashlib.md5(x).hexdigest() + '.jpg' for x in raw_list]
        for img_name in img_list:
            img_count = redis_dict.get(mid)
            if not img_count:
                new_name = mid + '_1.jpg'
                redis_dict.set(mid, "1")
            else:
                new_count = str(int(img_count) + 1)
                redis_dict.set(mid, new_count)
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
                data = (RELATION_TABLE, new_name, new_name.split('_', 1)[0], img_url, BUCKET_NAME, size,
                        img_name.replace('.jpg', ''),
                        '',
                        'machine', '1', 'online')
                copyfile(src_path + img_name, dst_path + new_name)
                insert_db(data)
                if count % 500 == 0:
                    print(data)
    print(count)
