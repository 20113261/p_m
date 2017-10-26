#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/23 上午11:02
# @Author  : Hou Rong
# @Site    : 
# @File    : crawl_data_check_script.py
# @Software: PyCharm
import pymysql
import datetime
import dataset
import json
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict
from data_source import MysqlSource
from service_platform_conn_pool import service_platform_pool
from logger import get_logger

logger = get_logger("crawl_data_check")

dev_ip = '10.10.69.170'
dev_user = 'reader'
dev_passwd = 'miaoji1109'
dev_db = 'base_data'
# dev_conn = pymysql.connect(host=dev_ip, user=dev_user, charset='utf8', passwd=dev_passwd, db=dev_db)
# dev_cursor = dev_conn.cursor()

ori_ip = '10.10.228.253'
ori_user = 'mioji_admin'
ori_password = 'mioji1109'
ori_db_name = 'BaseDataFinal'

city_id_count = defaultdict(int)
city_map_info_error_id_set = set()
distance_set = set()

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
    logger.debug('[cid size: {}]'.format(len(cid2map)))
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


def is_legal(s):
    if s:
        if isinstance(s, str):
            if s.strip():
                if s.lower() != 'null':
                    return True
        elif isinstance(s, int):
            if s > -1:
                return True

        elif isinstance(s, float):
            if s > -1.0:
                return True
    return False


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def insert_error_map_info_task(duplicate_map_info_set, task_table, task_type):
    # todo 当前由于 qyer 的数据表小，可以全量扫描，之后增加其他表的时候，需要修改此方法
    data = []
    # get all task info
    for duplicate_map_info in chunks(list(duplicate_map_info_set), 5000):
        _conn = service_platform_pool.connection()
        _cursor = _conn.cursor()
        if task_type == 'hotel':
            # 酒店数据不可用
            query_sql = '''SELECT
  source,
  source_id,
  address
FROM {}
WHERE map_info IN ({});'''.format(task_table, ",".join(
                map(lambda x: "'{}'".format(x),
                    filter(lambda x: map_info_legal(x), duplicate_map_info)
                    )
            ))
        elif task_type in ('attr', 'shop', 'rest', 'total'):
            query_sql = '''SELECT
  source,
  id,
  address
FROM {}
WHERE map_info IN ({});'''.format(task_table, ",".join(
                map(lambda x: "'{}'".format(x),
                    filter(lambda x: map_info_legal(x), duplicate_map_info)
                    )
            ))
        else:
            continue
        _cursor.execute(query_sql)

        # get all data
        for line in _cursor.fetchall():
            if not is_legal(line[2]):
                continue
            data.append(
                (
                    task_table,
                    line[0],
                    line[1],
                    json.dumps({
                        'address': line[2]
                    })
                )
            )
        _cursor.close()
        _conn.close()

    # insert all data
    _conn = service_platform_pool.connection()
    _cursor = _conn.cursor()
    _cursor.executemany(
        '''INSERT IGNORE INTO supplement_field (`table_name`, `type`, `source`, `sid`, `other_info`) VALUES (%s, 'map_info', %s, %s, %s)''',
        data)
    _conn.commit()
    _cursor.close()
    _conn.close()


def detectOriData():
    local_conn = pymysql.connect(host=ori_ip, user=ori_user, charset='utf8', passwd=ori_password, db=ori_db_name)
    city_map_info_dict = get_city_map()
    dt = datetime.datetime.now()

    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'BaseDataFinal';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    report_data = []
    for cand_table in table_list:
        cand_list = cand_table.split('_')
        # 使用 BaseDataFinal 中的数据进行数据校验，跳过不为 3 的表
        # 其中的数据表名称类似 attr_final_20170929a
        if len(cand_list) != 3:
            continue

        task_type, _, task_tag = cand_list

        # 跳过非这四种抓取任务类型
        if task_type not in ('attr', 'rest', 'hotel', 'total'):
            continue

        logger.debug(('[Begin Detect][table: {}]'.format(cand_table)))
        error_count = {}
        source_count = defaultdict(int)
        error_dict = defaultdict(int)

        if task_type == 'hotel':
            # 酒店类型
            sql = '''SELECT
      hotel_name,
      hotel_name_en,
      source,
      source_id,
      city_id,
      map_info,
      grade
    FROM {};'''.format(cand_table)

        elif task_type in ('attr', 'shop', 'rest', 'total'):
            # 景点、购物，餐厅当前 daodao 使用，以及全部 POI，qyer 使用
            sql = '''SELECT
      name,
      name_en,
      source,
      id,
      city_id,
      map_info,
      grade
    FROM {};'''.format(cand_table)
        else:
            # 未知类型，当前跳过
            continue

        # 获取数据，使用迭代的方式获得
        datas = MysqlSource(db_config={
            'host': ori_ip,
            'user': ori_user,
            'passwd': ori_password,
            'db': ori_db_name
        }, table_or_query=sql, size=10000, is_table=False)

        # 经纬度记录集合，用于判定重复内容
        map_info_set = defaultdict(set)

        # 重复经纬度集合，用于提取重复经纬度，以及在经纬度重复的值的最后添加 len(duplicate_map_info_set) 值，
        # 以保证返回值不会丢失第一次出现的 map_info
        duplicate_map_info_set = defaultdict(set)

        total = 0
        success = 0
        for data in datas:
            # 该条数据情况，数据正确，默认为 True，后续流程中会修改为 False
            right = True

            total += 1

            if total % 10000 == 0:
                logger.debug("[table data detect][table: {}][count: {}]".format(cand_table, total))
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

            # 增加本表中抓取源的统计
            source_count[source] += 1

            # # todo 从数据库中读取所有的抓取源
            # 抓取源错误为大错误，由于爬虫写错导致，应该发报警邮件
            # if source not in ('agoda', 'booking', 'ctrip', 'elong ', 'expedia', 'hotels'):
            #     error_dict['数据源错误'] += 1
            #     right = False

            if '' == name and '' == name_en:
                error_dict[(source, '无 name、name_en')] += 1
                right = False

            if '' != name and '' != name_en and is_contain_ch(name_en):
                if is_full_contain_ch(name_en):
                    if not is_contain_ch(name):
                        error_dict[(source, "中英文名字相反")] += 1
                        right = False

            if name.strip().lower() != name_en.strip().lower() \
                    and is_contain_ch(name) \
                    and not is_contain_ch(name_en) \
                    and len(name_en.split(' ')) >= 2 \
                    and name_en in name:
                error_dict[(source, "中文名中含有英文名")] += 1
                right = False

            if 'NULL' == map_info:
                error_dict[(source, '坐标错误(NULL)')] += 1
                right = False
            elif not map_info_legal(map_info):
                error_dict[(source, '坐标错误(坐标为空或坐标格式错误，除去NULL)')] += 1
                right = False
            else:
                # 经纬度重复情况判定
                if map_info in map_info_set[source]:
                    error_dict[(source, "经纬度重复")] += 1
                    if error_dict[(source, "经纬度重复")] == 1:
                        # 当此经纬度出现 1 次时，经纬度重复加 2 ，之后正常
                        error_dict[(source, "经纬度重复")] += 1
                    duplicate_map_info_set[source].add(map_info)
                    right = False

                # 当前情况为 map_info 为正确的情况，经纬度集合添加 map_info
                map_info_set[source].add(map_info)

                # 当城市经纬度合法时计算相应的距离
                city_map_info = city_map_info_dict.get(cid, None)
                if map_info_legal(city_map_info):

                    cand_dist = getDistByMap(city_map_info, map_info)
                    cand_reverse_dist = getDistByMap(city_map_info, ','.join(map_info.strip(',')[::-1]))

                    if cand_dist and cand_reverse_dist:
                        if cand_dist >= filter_dist:
                            right = False
                            error_dict[(source, '坐标与所属城市距离过远')] += 1
                            if cand_reverse_dist <= filter_dist:
                                error_dict[(source, "距离过远坐标翻转后属于所属城市")] += 1
                            else:
                                distance_set.add(sid)

            try:
                grade_f = float(grade)
                if grade_f > 10:
                    error_dict[(source, '静态评分异常(评分高于10分)')] += 1
                    right = False
            except:
                pass

            success += 1 if right else 0

        # 经纬度重复的值的最后添加 len(duplicate_map_info_set) 值
        # 以保证返回值不会丢失第一次出现的 map_info
        # todo duplicate map info fix source
        # error_dict[(source, '经纬度重复')] += len(duplicate_map_info_set)

        # 生成经纬度重复任务，当前只有 qyer
        if task_type in ('total', 'attr', 'rest'):
            for source, each_duplicate_map_info in duplicate_map_info_set.items():
                # get each detail table name
                detail_table = '_'.join(['detail', task_type, source, task_tag])
                insert_error_map_info_task(duplicate_map_info_set=each_duplicate_map_info,
                                           task_table=detail_table,
                                           task_type=task_type)

        logger.debug(
            "[table detected: {}][total: {}][error: {}][succeed: {}]".format(cand_table, total, error_count, success))

        for each_source, _c in source_count.items():
            report_data.append({
                'tag': task_tag,
                'source': each_source,
                'type': task_type,
                'error_type': '全量',
                'num': _c,
                'date': datetime.datetime.strftime(dt, '%Y%m%d'),
                'hour': datetime.datetime.strftime(dt, '%H'),
                'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
            })

        for s_err_type, num in error_dict.items():
            _source, _err_type = s_err_type
            report_data.append({
                'tag': task_tag,
                'source': _source,
                'type': task_type,
                'error_type': _err_type,
                'num': num,
                'date': datetime.datetime.strftime(dt, '%Y%m%d'),
                'hour': datetime.datetime.strftime(dt, '%H'),
                'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
            })

    db = dataset.connect('mysql+pymysql://mioji_admin:mioji1109@10.10.228.253/Report?charset=utf8')
    crawl_report_table = db['serviceplatform_crawl_report_summary']
    # serviceplatform_crawl_report_summary
    for each_data in report_data:
        try:
            crawl_report_table.upsert(each_data, keys=['tag', 'source', 'type', 'error_type', 'date'],
                                      ensure=None)
            logger.debug("[table_data: {}]".format(each_data))
        except Exception as exc:
            logger.exception(msg="[update report table error]",exc_info=exc)
    db.commit()
    logger.debug('Done')


if __name__ == '__main__':
    detectOriData()

    #     < !--  # error_key = ['全量','数据源错误', '无 name、name_en', "中英文名字相反", "中文名中含有英文名", '坐标错误(NULL)',-->
    # < !--  # '坐标错误(坐标为空或坐标格式错误，除去NULL)', "经纬度重复", '坐标与所属城市距离过远', "距离过远坐标翻转后属于所属城市",-->
    # < !--  # '静态评分异常(评分高于10分)']-->
