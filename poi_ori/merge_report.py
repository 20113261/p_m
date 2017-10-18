# encoding=utf8
import pandas
import datetime
from pymysql.cursors import DictCursor
from service_platform_conn_pool import base_data_pool, poi_ori_pool
from logger import get_logger

logger = get_logger("merge_report")
table_name = None
poi_name = None


def init_global_name(poi_type):
    global table_name
    global poi_name

    if poi_type == 'attr':
        poi_name = '景点'
        table_name = 'chat_attraction'
    elif poi_type == 'shop':
        poi_name = '购物'
        table_name = 'chat_shopping'
    elif poi_type == 'rest':
        poi_name = '餐厅'
        table_name = 'chat_restaurant'
    else:
        raise TypeError("Unknown Type: {}".format(poi_type))


def init_city_dict():
    conn = base_data_pool.connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT *
FROM (SELECT
        id,
        city.name      AS name,
        city.name_en   AS name_en,
        country.name   AS country,
        CASE WHEN grade = -1
          THEN 100
        ELSE grade END AS grade
      FROM city
        JOIN country ON city.country_id = country.mid
      ORDER BY grade) t
ORDER BY t.grade;''')
    for line in cursor.fetchall():
        # id, name, name_en, country, grade
        yield line
    cursor.close()
    conn.close()


def poi_merged_report(poi_type):
    init_global_name(poi_type)

    conn = poi_ori_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)

    get_column_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name='{}'".format(
        table_name)
    cursor.execute(get_column_sql)

    datas = cursor.fetchall()

    column_list = []

    filter_info = []
    error_data_id = []

    for data in datas:
        column_list.append(data['column_name'])

    report_data = pandas.DataFrame(
        columns=['ID', '城市名', '国家', '城市 grade', '开发库中{}个数'.format(poi_name), 'test 上线{}个数'.format(poi_name),
                 '上线{}占比'.format(poi_name), '上线{}有图占比'.format(poi_name), '开发库中 daodao {}个数'.format(poi_name),
                 '上线 daodao 个数', '上线 daodao {}占比'.format(poi_name)])
    report_count = 0
    for cid, city_name, city_name_en, country, grade in init_city_dict():
        get_sql = "select * from {} where city_id = '{}'".format(table_name, cid)
        cursor.execute(get_sql)
        datas = cursor.fetchall()
        name_null_fail_count = 0
        city_null_fail_count = 0
        first_img_null_fail_count = 0
        norm_tag_null_fail_count = 0
        map_fail_count = 0

        img_succeed_count = 0
        daodao_count = 0
        daodao_succeed_count = 0
        success_count = 0
        for data in datas:
            # daodao 统计部分
            if 'daodao' in data['data_source']:
                daodao_count += 1

            # 数据检查部分

            # name
            if data['name'].lower() in ('', 'null', '0') and data['name_en'] in ('', 'null', '0'):
                name_null_fail_count += 1
                error_data_id.append(data['id'])
                continue

            # city
            if data['city_id'] in ('', 'null', '0'):
                city_null_fail_count += 1
                error_data_id.append(data['id'])
                continue

            # map_info
            try:
                lat = float(data['map_info'].strip().split(',')[0])
                lgt = float(data['map_info'].strip().split(',')[1])
            except Exception:
                map_fail_count += 1
                error_data_id.append(data['id'])
                continue

            if 'daodao' in data['data_source']:
                daodao_succeed_count += 1

            success_count += 1

        if success_count != len(datas):
            filter_info.append((cid, city_name, country))

        report_data.loc[report_count] = [
            cid,
            city_name,
            country,
            grade,
            len(datas),
            success_count,
            "{0:04f}%".format(100 * success_count / float(len(datas))) if len(datas) != 0 else '无穷大',
            "{0:04f}%".format(100 * img_succeed_count / float(success_count)) if success_count != 0 else '无穷大',
            daodao_count,
            daodao_succeed_count,
            "{0:04f}%".format(
                100 * daodao_succeed_count / float(daodao_count)) if daodao_count != 0 else '无穷大'
        ]
        report_count += 1
        logger.debug(' '.join([cid, city_name, city_name_en, country]) + '\tall:' + str(len(datas)) + "\tget:" + str(
            success_count) + '\tname_null:' + str(
            name_null_fail_count) + '\tcity_null:' + str(city_null_fail_count) + '\timg_null:' + str(
            first_img_null_fail_count) + '\tmap:' + str(map_fail_count) + '\tnorm_tag:' + str(norm_tag_null_fail_count))

    for each in filter_info:
        logger.debug("[data_filter: {}]".format(each))

    logger.debug("[error data][id: {}]".format(error_data_id))

    pandas.DataFrame(report_data).to_excel(datetime.datetime.now().strftime('{}_report_%Y_%m_%d.xlsx'.format(poi_name)))
    pandas.DataFrame(report_data).to_csv(datetime.datetime.now().strftime('{}_report_%Y_%m_%d.csv'.format(poi_name)))


if __name__ == '__main__':
    import sys

    poi_type = sys.argv[1]
    poi_merged_report(poi_type)
