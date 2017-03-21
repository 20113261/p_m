import pymysql
import dataset
import pandas
from collections import defaultdict

TEST_SQL_DICT = {
    'host': '10.10.111.62',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

ONLINE_SQL_DICT = {
    'host': '10.10.222.209',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

KEYS = [
    "name", "name_en", "alias", "continent", "country", "region", "map_info", "visit_num", "trans_degree", "grade",
    "dur", "time_zone", "summer_zone", "summer_start", "summer_end", "summer_start_next_ƒyear", "summer_end_next_year",
    "test_newProduct_status", "online_newProduct_status", "transit_visa", "tri_code", "city_type",
    "border_map_1", "border_map_2", "newProduct_dept_status", "new_product_city_pic", "region_1", "country_id",
    "continent_id", "is_park", "region_cn", "region_en"
]


def get_columns():
    __name = set()
    conn = pymysql.connect(**ONLINE_SQL_DICT)
    with conn as cursor:
        cursor.execute('''SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'base_data' AND table_name = '{}';'''.format(TABLE_NAME))
        for line in cursor.fetchall():
            __name.add(line[0])
    conn.close()
    return __name


if __name__ == '__main__':
    # ---- START ----
    TABLE_NAME = 'city'
    ID_LINE = 'id'
    # ---------------

    count_dict = defaultdict(int)
    # diff_dict = dict()
    diff_list = []

    cols = get_columns()
    had_keys = []
    for key in KEYS:
        if key in cols:
            had_keys.append(key)
    test_db = dataset.connect('mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**TEST_SQL_DICT))
    test_table = test_db[TABLE_NAME].all()

    online_db = dataset.connect('mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**ONLINE_SQL_DICT))
    online_table = online_db.query('select * from city where newProduct_status="Open"')

    test_dict = dict()

    for line in test_table:
        test_dict[line[ID_LINE]] = line

    for line in online_table:
        mid = line[ID_LINE]
        test_line = test_dict[mid]
        for key in had_keys:
            test_content = test_line[key]
            online_content = line[key]
            if test_content != online_content:
                count_dict[key] += 1
                # print('#' * 100)
                # print(mid)
                # print(key, test_content, online_content)
                # diff_dict[mid] = (key, test_content, online_content)
                diff_list.append([mid, key, test_content, online_content])

    print(count_dict)
    pd = pandas.DataFrame(diff_list, columns=['mid', 'key', 'test', 'online'])
    print(pd.to_excel('/tmp/report_city.xlsx'))
