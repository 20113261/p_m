#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/23 上午11:02
# @Author  : Hou Rong
# @Site    : 
# @File    : crawl_data_check_script.py
# @Software: PyCharm
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 上午11:38
# @Author  : Hou Rong
# @Site    :
# @File    : hotel_check.py
# @Software: PyCharm
import pymysql
import datetime
import dataset
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict

dev_ip = '10.10.69.170'
dev_user = 'reader'
dev_passwd = 'miaoji1109'
dev_db = 'base_data'

dev_conn = pymysql.connect(host=dev_ip, user=dev_user, charset='utf8', passwd=dev_passwd, db=dev_db)
dev_cursor = dev_conn.cursor()

ori_ip = '10.10.228.253'
ori_user = 'mioji_admin'
ori_password = 'mioji1109'
ori_db_name = 'ServicePlatform'

city_id_count = defaultdict(int)
city_map_info_error_id_set = set()
distance_set = set()
map_info_set = set()
duplicate_map_info_set = set()

filter_dist = 500


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r


def getDistByMap(map_info_1, map_info_2):
    try:
        lon1 = float(map_info_1.split(',')[0])
        lat1 = float(map_info_1.split(',')[1])

        lon2 = float(map_info_2.split(',')[0])
        lat2 = float(map_info_2.split(',')[1])

        return haversine(lon1, lat1, lon2, lat2)

    except Exception as e:
        return None


def get_city_map():
    conn = pymysql.connect(host=dev_ip, user=dev_user, passwd=dev_passwd, db=dev_db, charset='utf8')
    cursor = conn.cursor()

    sql = "select id,map_info from city;"
    cursor.execute(sql)
    datas = cursor.fetchall()

    cid2map = {}

    for data in datas:
        if None in data:
            continue

        cid = data[0]
        map_info = data[1]

        try:
            map_info_list = map_info.strip().split(',')

            lat = float(map_info_list[0])
            lgt = float(map_info_list[1])

        except Exception as e:
            continue

        cid2map[cid] = map_info

    cursor.close()
    conn.close()
    print('cid size:' + str(len(cid2map)))
    return cid2map


def is_chinese(uchar):
    if '\u4e00' <= uchar <= "\u9fa5":
        return True
    else:
        return False


def is_contain_ch(ustr):
    for ch in ustr:
        if is_chinese(ch):
            return True

    return False


def is_full_contain_ch(ustr):
    for ch in ustr:
        if not is_chinese(ch):
            return False
    if ustr == "":
        return False
    return True


def map_info_legal(_map_info):
    try:
        lat, lng = _map_info.split(',')
        float(lat)
        float(lng)
        if ' ' in lat:
            raise Exception()
        if ' ' in lng:
            raise Exception()
        return True
    except Exception as e:
        return False


def detectOriData():
    local_conn = pymysql.connect(host=ori_ip, user=ori_user, charset='utf8', passwd=ori_password, db=ori_db_name)
    city_map_info_dict = get_city_map()
    dt = datetime.datetime.now()

    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    report_data = []
    for cand_table in table_list:
        cand_list = cand_table.split('_')
        # 跳过不为 4 的表
        if len(cand_list) != 4:
            continue

        crawl_type, task_type, cand_source, task_tag = cand_list

        # 跳过不是详情页的表
        if crawl_type != 'detail':
            continue

        print(('Begin ' + cand_table))
        error_count = {}
        error_dict = defaultdict(int)

        sql = '''SELECT
  hotel_name,
  hotel_name_en,
  source,
  source_id,
  city_id,
  map_info,
  grade
FROM {};'''.format(cand_table)

        local_cursor = local_conn.cursor()
        local_cursor.execute(sql)
        datas = local_cursor.fetchall()
        local_cursor.close()

        total = 0
        success = 0
        for data in datas:
            # 该条数据情况，数据正确，默认为 True，后续流程中会修改为 False
            right = True

            total += 1

            if total % 10000 == 0:
                print(total)
            word_list = []

            for word in data:
                if word is None:
                    word_list.append('')
                else:
                    word_list.append(word)

            name = word_list[0]
            name_en = word_list[1]
            source = word_list[2]
            sid = word_list[3]
            try:
                cid = str(int(word_list[4]))
            except Exception:
                cid = word_list[4]
            map_info = word_list[5]
            grade = word_list[6]

            if source != cand_source:
                error_dict['数据源错误'] += 1
                right = False

            if '' == name and '' == name_en:
                error_dict['无 name、name_en'] += 1
                right = False

            if '' != name and '' != name_en and is_contain_ch(name_en):
                if is_full_contain_ch(name_en):
                    if not is_contain_ch(name):
                        error_dict["中英文名字相反"] += 1
                        right = False

            if name.strip().lower() != name_en.strip().lower() \
                    and is_contain_ch(name) \
                    and not is_contain_ch(name_en) \
                    and len(name_en.split(' ')) >= 2 \
                    and name_en in name:
                error_dict["中文名中含有英文名"] += 1
                right = False

            if 'NULL' == map_info:
                error_dict['坐标错误(NULL)'] += 1
                right = False
            elif not map_info_legal(map_info):
                error_dict['坐标错误(坐标为空或坐标格式错误，除去NULL)'] += 1
                right = False
            elif map_info in map_info_set:
                error_dict["经纬度重复"] += 1
                duplicate_map_info_set.add(map_info)
                right = False
            else:
                # 当前情况为 map_info 为正确的情况，经纬度集合添加 map_info
                map_info_set.add(map_info)

                # 当城市经纬度合法时计算相应的距离
                city_map_info = city_map_info_dict.get(cid, None)
                if map_info_legal(city_map_info):

                    cand_dist = getDistByMap(city_map_info, map_info)
                    cand_reverse_dist = getDistByMap(city_map_info, ','.join(map_info.strip(',')[::-1]))

                    if cand_dist >= filter_dist:
                        right = False
                        error_dict['坐标与所属城市距离过远'] += 1
                        if cand_reverse_dist <= filter_dist:
                            error_dict["距离过远坐标翻转后属于所属城市"] += 1
                        else:
                            distance_set.add(sid)

            try:
                grade_f = float(grade)
                if grade_f > 10:
                    error_dict['静态评分异常(评分高于10分)'] += 1
                    right = False
            except:
                pass

            success += 1 if right else 0

        print(total, error_count, success, cand_table)
        print(cand_source, 'hotel', total, success)
        report_data.append({
            'tag': task_tag,
            'source': cand_source,
            'type': task_type,
            'error_type': '全量',
            'num': total,
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        })

        for error_type, num in error_dict.items():
            report_data.append({
                'tag': task_tag,
                'source': cand_source,
                'type': task_type,
                'error_type': error_type,
                'num': num,
                'date': datetime.datetime.strftime(dt, '%Y%m%d'),
                'hour': datetime.datetime.strftime(dt, '%H'),
                'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
            })
            # os.system('python3 green_report.py {0} hotel {1} {2}'.format(cand_source, total, success))
            # # 打印距离过远的点
            # print(distance_set)

    # serviceplatform_crawl_report_summary
    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    crawl_report_table = db['serviceplatform_crawl_report_summary']

    for each_data in report_data:
        try:
            crawl_report_table.upsert(each_data, keys=['tag', 'source', 'type', 'error_type', 'date'],
                                      ensure=None)
        except Exception:
            pass

        print(each_data)

    print('Done')


if __name__ == '__main__':
    detectOriData()

    #     < !--  # error_key = ['全量','数据源错误', '无 name、name_en', "中英文名字相反", "中文名中含有英文名", '坐标错误(NULL)',-->
    # < !--  # '坐标错误(坐标为空或坐标格式错误，除去NULL)', "经纬度重复", '坐标与所属城市距离过远', "距离过远坐标翻转后属于所属城市",-->
    # < !--  # '静态评分异常(评分高于10分)']-->
