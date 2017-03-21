import dataset
import pandas
import re
from collections import defaultdict

ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

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

SHOP_KEYS = ["name", "name_en", "address", "open", "map_info", "city", "fix_ranking", "norm_tagid"]
ATTR_KEYS = ["name", "name_en", "address", "alias", "open", "map_info", "intensity", "rcmd_intensity", "city",
             "introduction", "fix_ranking", "norm_tagid"]

if __name__ == '__main__':
    # ---- START ----
    TABLE_NAME = 'chat_attraction'
    ID_LINE = 'id'
    # ---------------

    count_dict = defaultdict(int)
    # diff_dict = dict()
    diff_list = []
    lost_id_list = []

    test_db = dataset.connect('mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**TEST_SQL_DICT))
    test_table = test_db[TABLE_NAME].all()

    online_db = dataset.connect('mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**ONLINE_SQL_DICT))
    online_table = online_db.query('select * from {} where online=1'.format(TABLE_NAME))

    test_dict = dict()

    for line in test_table:
        test_dict[line[ID_LINE]] = line

    _count = 0

    for line in online_table:
        mid = line[ID_LINE]
        try:
            test_line = test_dict[mid]
        except:
            lost_id_list.append(mid)
            continue
        for key in ATTR_KEYS:
            test_content = test_line[key]
            online_content = line[key]
            if test_content != online_content:
                count_dict[key] += 1
                _count += 1
                if _count % 100 == 0:
                    print(_count)
                # print('#' * 100)
                # print(mid)
                # print(key, test_content, online_content)
                # diff_dict[mid] = (key, test_content, online_content)
                diff_list.append([mid, key, ILLEGAL_CHARACTERS_RE.sub(r'', test_content),
                                  ILLEGAL_CHARACTERS_RE.sub(r'', online_content)])

    print(count_dict)
    pd = pandas.DataFrame(diff_list, columns=['mid', 'key', 'test', 'online'])
    print(pd)
    print(pd.to_excel('/tmp/report_attr.xlsx'))
    print(lost_id_list)
