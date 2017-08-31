# coding=utf-8
import json
import gc
import pymysql
import toolbox.Common
import copy
from pymysql.cursors import DictCursor
from collections import defaultdict
from add_open_time.add_open_time import fix_daodao_open_time
from norm_tag.rest_norm_tag import get_norm_tag

others_name_list = ['source']
json_name_list = ['commentcounts', 'ranking', 'price', 'introduction']
norm_name_list = ['name', 'name_en', 'map_info', 'address', 'grade', 'url', 'phone', 'opentime',
                  'prize', 'traveler_choice', 'price_level', 'cuisines', 'imgurl']

key_priority = defaultdict(int)
key_priority.update({
    'daodao': 10,
    'qyer': 9,
    'yelp': 8,
    'tripadvisor': 10
})

W2N = {
    '¥¥-¥¥¥': '23',
    '¥': '1',
    '': '0',
    '¥¥¥¥': '4'
}


def get_key_by_priority(src: dict):
    if len(src.keys()) == 0:
        return None
    for k, v in sorted(src.items(), key=lambda x: key_priority[x[0]], reverse=True):
        if toolbox.Common.is_legal(v):
            return v
    return None


def get_rest_dict(city_id):
    rest_dict = defaultdict(dict)

    # get s sid
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='rest_merge')
    with conn.cursor() as cursor:
        sql = '''SELECT
  source,
  source_id
FROM rest_unid
WHERE city_id = %s;'''
        cursor.execute(sql, (city_id,))
        res = ["('{0}', '{1}')".format(s, sid) for s, sid in cursor.fetchall()]
    conn.close()

    # 优化内存占用问题
    # get whole line in per city
    conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', passwd='mioji1109', charset='utf8', db='base_data')
    with conn.cursor(cursor=DictCursor) as cursor:
        sql = "select * from rest where (source, id) in (%s)" % ','.join(res)

        cursor.execute(sql)
        for line in cursor.fetchall():
            source = line['source']
            source_id = line['id']
            rest_dict[(source, source_id)] = line
    conn.close()
    return rest_dict


def get_task():
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='rest_merge')
    # 获取所有用于融合的城市 id
    cursor = conn.cursor()
    cursor.execute("select distinct city_id from rest_unid where city_id in ({});".format(
        ','.join((map(lambda x: x.strip(), open('cid_file'))))))
    total_city_id = list(map(lambda x: x[0], cursor.fetchall()))
    cursor.close()

    for each_city_id in total_city_id:
        print("City ID:", each_city_id)
        cursor = conn.cursor(cursor=DictCursor)
        city_rest_dict = defaultdict(list)
        sql = '''SELECT
  id,
  city_id,
  group_concat(concat(source, '|', source_id) SEPARATOR '|_||_|') AS union_info
FROM rest_unid WHERE city_id=%s
GROUP BY id'''
        cursor.execute(sql, (each_city_id,))
        for line in cursor.fetchall():
            miaoji_id = line['id']
            city_id = line['city_id']
            union_info = line['union_info']
            city_rest_dict[city_id].append((miaoji_id, city_id, union_info))
        yield city_rest_dict
        cursor.close()


if __name__ == '__main__':
    conn = pymysql.connect(host='10.10.180.145', user='hourong', passwd='hourong', charset='utf8', db='rest_merge')

    sql = 'replace into chat_restaurant(`id`,`name`,`name_en`,' \
          '`source`,`city_id`,`map_info`,`address`,`real_ranking`,' \
          '`grade`,`res_url`,`telphone`,`introduction`,`open_time`,`open_time_desc`,`prize`,' \
          '`traveler_choice`,`review_num`,`price`,`price_level`,`cuisines`, ' \
          '`image_urls`,`tagid`,`norm_tagid`,`norm_tagid_en`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
          '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

    for task_dict in get_task():
        count = 0
        data = []
        # 获取融合城市信息
        for key, values in list(task_dict.items()):
            # get per city rest info
            rest_info_dict = get_rest_dict(key)
            for miaoji_id, city_id, union_info in values:
                # 初始化融合前变量
                data_dict = defaultdict(dict)
                can_be_used = False

                # 初始化融合信息
                for each_name in (json_name_list + norm_name_list + others_name_list):
                    data_dict[each_name] = {}

                # 遍历所有需要融合的 source 以及 id，并生成 dict 类融合内容
                for s_sid in union_info.split('|_||_|'):
                    source, source_id = s_sid.split('|')

                    # 未获得融合 id 信息
                    if not source_id or not source:
                        continue

                    # 未获得融合数据
                    rest_info = rest_info_dict[(source, source_id)]
                    if rest_info == {}:
                        continue

                    can_be_used = True

                    for each_name in (json_name_list + norm_name_list):
                        if str(rest_info[each_name]).upper() not in ('NULL', ''):
                            data_dict[each_name][source] = rest_info[each_name]

                # 不能融合的内容包含两种
                if not can_be_used:
                    print('union_info', union_info)
                    continue

                data_dict['tagid'] = copy.deepcopy(data_dict['cuisines'])

                new_data_dict = {}
                for norm_name in norm_name_list:
                    new_data_dict[norm_name] = get_key_by_priority(data_dict[norm_name]) or ''

                for json_name in (set(json_name_list) | {'tagid', }):
                    new_data_dict[json_name] = json.dumps(data_dict[json_name])

                new_data_dict['phone'] = new_data_dict['phone'].replace('电话号码：', '').strip()

                # 添加 source
                source = '|'.join(sorted(data_dict['source'].values()))

                # todo modify opentime, norm_tagid, comment and so on
                if 'daodao' in data_dict['opentime']:
                    open_desc = data_dict['opentime']['daodao']
                    try:
                        norm_open_time = fix_daodao_open_time(open_desc)
                    except Exception:
                        print(open_desc)
                else:
                    norm_open_time = ''

                if 'daodao' in data_dict['tagid']:
                    norm_tag, norm_tag_en = get_norm_tag(data_dict['tagid'])
                else:
                    norm_tag, norm_tag_en = '', ''

                # 替换旧的 data_dict
                data_dict = new_data_dict

                # phone 处理
                if data_dict['phone'] == '+ 新增電話號碼':
                    data_dict['phone'] = ''

                # price_level
                data_dict['price_level'] = W2N.get(data_dict.get('price_level', ''), '0')

                data.append((
                    miaoji_id, data_dict['name'], data_dict['name_en'], source, key,
                    data_dict['map_info'],
                    data_dict['address'],
                    data_dict['ranking'], data_dict['grade'],
                    data_dict['url'], data_dict['phone'], data_dict['introduction'], norm_open_time,
                    data_dict['opentime'],
                    data_dict['prize'], data_dict['traveler_choice'],
                    data_dict['commentcounts'], data_dict['price'], data_dict['price_level'], data_dict['cuisines'],
                    data_dict['imgurl'], data_dict['tagid'], norm_tag, norm_tag_en))

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
