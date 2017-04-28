import pandas
import pymysql
import dataset


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


def update_table(table, table_name, search_keys=None, ignore_cols=None, debug=False):
    if search_keys is None:
        search_keys = []
    if ignore_cols is None:
        ignore_cols = []

    cols = get_columns()

    # 判断，为库中列名以及不是忽略更新的列的名称
    keys = list(
        filter(lambda x: x is not None,
               [key
                if (
                   key in cols and key not in ignore_cols and key not in search_keys
               ) else None
                for key in table.keys()
                ]
               )
    )

    # 生成 Update Sql
    sql = 'UPDATE {2} SET {0} WHERE {1}'.format(
        ','.join(
            map(lambda x: '`' + x + '`=%s', keys)
        ),
        ' AND '.join(
            map(lambda x: '`' + x + '`=%s', search_keys)
        ),
        table_name,
    )
    print(sql)
    data_total = []
    for i in range(len(table)):
        line = table.irow(i)
        data = [line[k] for k in (keys + search_keys)]
        data_total.append(data)

    if not debug:
        conn = pymysql.connect(**SQL_DICT)
        cursor = conn.cursor()
        print('#' * 100)
        print('数据，共', len(data_total))
        print('更新，共', cursor.executemany(sql, data_total))
        print('#' * 100)
        conn.close()
    else:
        print(data_total)


def upsert_table(table, table_name, search_keys=None, ignore_cols=None, debug=False):
    db = dataset.connect('mysql+pymysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**SQL_DICT))
    dataset_table = db[table_name]
    for i in range(len(table)):
        row = dict(table.irow(i))
        for j in ignore_cols:
            try:
                row.pop(j)
            except Exception:
                continue

        for k in row:
            row[k] = row[k].strip()

        if debug:
            print(row)
        else:
            print(dataset_table.upsert(row, search_keys))


if __name__ == '__main__':
    # Sheet 1 - 修改visit_num
    # Sheet 1 - 删除城市_从库里直接删除
    # Sheet 1 - 修改region相关字段
    # Sheet 1 - 时区

    SQL_DICT = {
        'host': '10.10.154.38',
        'user': 'writer',
        'password': 'miaoji1109',
        'charset': 'utf8',
        'db': 'devdb'
    }
    xlsx_path = '/Users/hourong/Downloads/新增国家_0411.xlsx'
    sheetname = '工作表 1'
    header = 1
    table_name = 'country'
    search_keys = ['mid']
    ignore_cols = []

    table = pandas.read_excel(
        xlsx_path,
        sheetname=sheetname,
        header=header
    )
    converters = {key: str for key in table.keys()}
    table = pandas.read_excel(
        xlsx_path,
        sheetname=sheetname,
        header=header,
        converters=converters
    ).fillna('NULL')

    upsert_table(table, table_name, search_keys=search_keys, ignore_cols=ignore_cols, debug=False)

    # update_table(table, table_name, search_keys=search_keys, ignore_cols=ignore_cols, debug=False)
