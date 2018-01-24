import pandas
import pymysql
pymysql.install_as_MySQLdb()
import dataset
import csv
from city.config import base_path
from collections import defaultdict
import json
import traceback
from logger import get_logger
# SQL_DICT = {
#     'host': '10.10.230.206',
#     'user': 'mioji_admin',
#     'password': 'mioji1109',
#     'charset': 'utf8',
#     'db': 'tmp'
# }
SQL_DICT = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}
logger = get_logger('city')
ALL_NULL = ['NULL', 'Null', 'null', None, '', 'None', ' ']

change_map_info_key = ['map_info', 'border_map_1', 'border_map_2']
need_change_map_info = True


def get_columns():
    __name = set()
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{0}' AND table_name = 'city';'''.format(SQL_DICT['db']))
        for line in cursor.fetchall():
            __name.add(line[0])
    conn.close()
    return __name


def get_max_id() -> int:
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT max(id)
FROM city
WHERE id NOT LIKE '9%';''')
        for __max_id in cursor.fetchall():
            return int(__max_id[0])


max_id = get_max_id()


def generate_id(country_id: str) -> str:
    global max_id
    # continent_id = (int(country_id) // 100) * 10
    # continent_id = str(continent_id)
    max_id += 1
    return max_id
    # continent_max_id_dict[continent_id] += 1
    # return str(continent_max_id_dict[str(continent_id)])


def get_country_id_dict() -> dict:
    __dict = {}
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('SELECT mid, name from country')
        for mid, name in cursor.fetchall():
            __dict[name] = mid
    return __dict


def check_and_modify_columns(key: str, value: str) -> (bool, str):
    _value = value.strip()
    if key in ['newProduct_status', 'newProduct_dept_status', 'status', 'dept_status']:
        return True, _value.title()

    if key in change_map_info_key:
        map_info = _value.replace('，', ',').replace(' ', '')
        if need_change_map_info:
            map_info = ','.join((map_info.split(',')[::-1]))
        return True, map_info

    if key in ['time_zone', 'summer_zone']:
        tmp = _value
        if len(tmp) > 0 and tmp[0] == '+':
            tmp = tmp[1:]
        if tmp.find(':') > 0:
            tmp = str(int(tmp.split(':')[0]) + float(tmp.split(':')[1]) / 60)
        return True, tmp

    if _value in ALL_NULL:
        if key in ['name', 'name_en']:
            return True, 'NULL'
        else:
            return False, None

    return True, _value


def read_file(xlsx_path,config):

    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        global change_map_info_key
        # change_map_info_key = ['border_map_1', 'border_map_2']
        change_map_info_key = ['map_info']

        debug = False
        target_db = 'mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**config)
        target_table = 'city'

        all_city_id = []
        cols = get_columns()
        country_id_dict = get_country_id_dict()
        header = 1
        sheetname = 'city表'
        table = pandas.read_excel(
            xlsx_path,
            header=header,
            sheetname=sheetname,
        ).fillna('null')

        data_table = dataset.connect(target_db).get_table(target_table)

        converters = {key: str for key in table.keys()}
        table = pandas.read_excel(
            xlsx_path,
            header=header,
            sheetname=sheetname,
            converters=converters,
        ).fillna('null')

        conn = pymysql.connect(**SQL_DICT)
        with conn as cursor:
            for i in range(len(table)):
                line = table.iloc[i]
                data = {}
                for key in table.keys():
                    # 去除无关项
                    if key not in cols:
                        continue

                    # 去除中英文名为空的
                    # if line['name'] in ALL_NULL and line['name_en'] in ALL_NULL:
                    #     continue
                    # 判断字段是否符合规范
                    res, value = check_and_modify_columns(key, line[key])
                    if res:
                        if value not in ('NULL', 'null', ''):
                            data[key] = value
                # 去除无用行
                if not data:
                    continue

                # 补充字段
                if 'id' not in data.keys():
                    data['id'] = generate_id(data['country_id'])
                    if 'country_id' not in data.keys():
                        data['country_id'] = country_id_dict[data['country']]

                if 'visit_num' not in data.keys():
                    data['visit_num'] = '0'

                # 补全必须字段
                if 'region_id' not in data.keys():
                    data['region_id'] = 'NULL'

                if debug:
                    print(data)
                else:
                    data_table.upsert(data, keys=['id'])
                all_city_id.append(data['id'])
        with open(base_path+'city_id.csv','w+') as city:
            writer = csv.writer(city)
            writer.writerow(("city_id",))
            for city_id in all_city_id:
                writer.writerow((city_id,))
        return_result = json.dumps(return_result)
        logger.debug("[return][{0}]".format(return_result))
        return all_city_id
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        logger.debug("[return][{0}]".format(return_result))

if __name__ == '__main__':
    # xlsx_path = '/search/tmp/大峡谷分隔城市及机场.xlsx'
    # xlsx_path = '/tmp/new_city.xlsx'
    # xlsx_path = '/Users/hourong/Downloads/需要修改的城市信息.xlsx'
    # xlsx_path = '/Users/hourong/Downloads/meizhilv.xlsx'
    xlsx_path = '/data/city/'
    read_file(xlsx_path)
