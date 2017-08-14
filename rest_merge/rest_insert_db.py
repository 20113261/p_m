# coding=utf-8
import json
import gc
import pymysql
from pymysql.cursors import DictCursor

json_name_list = ['review_num', 'real_ranking', 'price', 'description']
norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'res_url', 'telphone', 'open_time',
                  'prize', 'traveler_choice', 'price_level', 'cuisines', 'image_urls']


def get_rest_dict(rest_id_list):
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='rest_merge')
    with conn.cursor(cursor=DictCursor) as cursor:
        rest_dict = {}
        sql = "select * from rest_merge.rest where id in (%s)" % ','.join(
            ["\"" + x + "\"" for x in rest_id_list])
        cursor.execute(sql)
        for line in cursor.fetchall():
            rest_dict[line['id']] = line
    conn.close()
    return rest_dict


def get_task():
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='rest_merge')

    # 获取所有用于融合的城市 id
    cursor = conn.cursor()
    cursor.execute("select distinct city_id from rest_unid")
    total_city_id = list(map(lambda x: x[0], cursor.fetchall()))
    cursor.close()

    for each_city_id in total_city_id:
        print("City ID:", each_city_id)

        cursor = conn.cursor(cursor=DictCursor)
        city_rest_dict = {}
        sql = "select * from rest_merge.rest_unid where city_id=%s"
        cursor.execute(sql, (each_city_id,))
        for line in cursor.fetchall():
            miaoji_id = line['id']
            source = line['source']
            source_id = line['source_id']
            city_id = line['city_id']
            city_name = line['city_name']
            country_name = line['country_name']
            if city_id not in city_rest_dict:
                city_rest_dict[city_id] = []
            city_rest_dict[city_id].append((miaoji_id, source, source_id, city_name, country_name))
        yield city_rest_dict
        cursor.close()


if __name__ == '__main__':
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='rest_merge')

    for task_dict in get_task():
        count = 0
        data = []
        sql = 'insert ignore into rest_merge.restaurant_tmp(`id`,`name`,`name_en`,`source`,`city_id`,`city`,`map_info`,`address`,`real_ranking`,`grade`,`res_url`,`telphone`,`introduction`,`open_time_desc`,`prize`,`traveler_choice`,`review_num`,`price`,`price_level`,`cuisines`, `image_urls`,`tagid`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        # 获取融合城市信息
        for key, values in list(task_dict.items()):
            rest_info_dict = get_rest_dict([x[2] for x in values])
            for rest_key_info in values:
                data_dict = {}
                miaoji_id = rest_key_info[0]
                source = rest_key_info[1]
                source_id = rest_key_info[2]
                city_name = rest_key_info[3]
                country_name = rest_key_info[4]

                try:
                    rest_info = rest_info_dict[source_id]
                except Exception as e:
                    print(str(e))
                    continue

                for norm_name in norm_name_list:
                    val = rest_info[norm_name] or ''
                    if val != 'NULL':
                        data_dict[norm_name] = val
                    else:
                        data_dict[norm_name] = ''

                for json_name in json_name_list:
                    data_dict[json_name] = {}

                for json_name in json_name_list:
                    data_dict[json_name][source] = rest_info[json_name] or ''

                for json_name in json_name_list:
                    data_dict[json_name] = json.dumps(data_dict[json_name])

                data_dict['tagid'] = json.dumps({source: rest_info.get('cuisines', '') or ''})

                data_dict['telphone'] = data_dict['telphone'].replace('电话号码：', '').strip()

                data.append((
                    miaoji_id, data_dict['name'], data_dict['name_en'], source, key, city_name,
                    data_dict['map_info'],
                    data_dict['address'],
                    data_dict['real_ranking'], data_dict['grade'],
                    data_dict['res_url'], data_dict['telphone'], data_dict['description'], data_dict['open_time'],
                    data_dict['prize'], data_dict['traveler_choice'],
                    data_dict['review_num'], data_dict['price'], data_dict['price_level'], data_dict['cuisines'],
                    data_dict['image_urls'], data_dict['tagid']))

                if count % 3000 == 0:
                    print("Total:", count)
                    cursor = conn.cursor()
                    print("Insert:", cursor.executemany(sql, data))
                    cursor.close()
                    data = []
                count += 1

        print("Total:", count)
        cursor = conn.cursor()
        print("Insert:", cursor.executemany(sql, data))
        cursor.close()
        gc.collect()
    conn.close()
