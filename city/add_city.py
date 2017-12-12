import pandas
import pymysql
import dataset

# SQL_DICT = {
#     'host': '10.10.230.206',
#     'user': 'mioji_admin',
#     'password': 'mioji1109',
#     'charset': 'utf8',
#     'db': 'tmp'
# }
SQL_DICT = {
    'host': '10.10.228.253',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

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


def get_continent_max_id_dict() -> dict:
    _dict = {}
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT
  country.continent_id,
  MAX(id) AS max_id
FROM `city`
  JOIN country ON city.country_id = country.mid
WHERE id NOT LIKE '9%' AND id != 60002
GROUP BY country.continent_id;''')
        for continent, max_id in cursor.fetchall():
            _dict[str(continent)] = int(max_id)
    return _dict


continent_max_id_dict = get_continent_max_id_dict()


def generate_id(country_id: str) -> str:
    continent_id = (int(country_id) // 100) * 10
    continent_id = str(continent_id)
    continent_max_id_dict[continent_id] += 1
    return str(continent_max_id_dict[str(continent_id)])


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

def read_file(xlsx_path):

    global change_map_info_key
    # change_map_info_key = ['border_map_1', 'border_map_2']
    change_map_info_key = ['map_info']

    debug = False
    target_db = 'mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**SQL_DICT)
    target_table = 'city'

    all_city_id = []
    cols = get_columns()
    country_id_dict = get_country_id_dict()
    header = 1
    sheetname = '工作表 1'
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

    print(all_city_id)

if __name__ == '__main__':
    # xlsx_path = '/search/tmp/大峡谷分隔城市及机场.xlsx'
    # xlsx_path = '/tmp/new_city.xlsx'
    # xlsx_path = '/Users/hourong/Downloads/需要修改的城市信息.xlsx'
    # xlsx_path = '/Users/hourong/Downloads/meizhilv.xlsx'
    xlsx_path = '/Users/hourong/Downloads/1116.xlsx'
    read_file(xlsx_path)

