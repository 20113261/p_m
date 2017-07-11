import pandas
import pymysql
import dataset

SQL_DICT = {
    'host': '10.10.230.206',
    'user': 'mioji_admin',
    'password': 'mioji1109',
    'charset': 'utf8',
    'db': 'tmp'
}
ALL_NULL = ['NULL', 'Null', 'null', None, '', 'None', ' ']


def get_columns():
    __name = set()
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'devdb' AND table_name = 'city';''')
        for line in cursor.fetchall():
            __name.add(line[0])
    conn.close()
    return __name


def get_continent_max_id_dict() -> dict:
    _dict = {}
    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT
  continent_id,
  MAX(id) AS max_id
FROM `city`
GROUP BY continent_id''')
        for continent, max_id in cursor.fetchall():
            _dict[str(continent)] = int(max_id)
    return _dict


continent_max_id_dict = get_continent_max_id_dict()


def generate_id(continent: str) -> str:
    continent_max_id_dict[continent] += 1
    return str(continent_max_id_dict[continent])


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

    if key in ['map_info', 'border_map_1', 'border_map_2']:
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


if __name__ == '__main__':
    xlsx_path = '/search/tmp/大峡谷分隔城市及机场.xlsx'
    need_change_map_info = False
    debug = False
    target_db = 'mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**SQL_DICT)
    target_table = 'city'

    all_city_id = []
    cols = get_columns()
    country_id_dict = get_country_id_dict()
    header = 1
    table = pandas.read_excel(
        xlsx_path,
        header=header
    )
    converters = {key: str for key in table.keys()}
    table = pandas.read_excel(
        xlsx_path,
        header=header,
        converters=converters,
    ).fillna('null')

    data_table = dataset.connect(target_db).get_table(target_table)

    conn = pymysql.connect(**SQL_DICT)
    with conn as cursor:
        for i in range(len(table)):
            line = table.irow(i)
            data = {}
            for key in table.keys():
                # 去除无关项
                if key not in cols:
                    continue

                # 去除中英文名为空的
                if line['name'] in ALL_NULL and line['name_en'] in ALL_NULL:
                    continue

                # 判断字段是否符合规范
                res, value = check_and_modify_columns(key, line[key])
                if res:
                    data[key] = value

            # 补充字段
            if 'id' not in data.keys():
                data['id'] = generate_id(data['continent_id'])
            if 'country_id' not in data.keys():
                data['country_id'] = country_id_dict[data['country']]
            if 'city_type' not in data.keys():
                data['city_type'] = 'Regular'

            if 'visit_num' not in data.keys():
                data['visit_num'] = '0'

            if debug:
                print(data)
            else:
                data_table.insert(data)
            all_city_id.append(data['id'])

    print(all_city_id)
