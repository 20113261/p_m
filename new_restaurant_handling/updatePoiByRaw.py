"""

一、以POI的ID为准，将开发库中没有进入online环境的全部POI（景点、购物点和餐厅）加入online环境：
提sql前请注意：
1. 检查open字段。如为空，则统一填为：<*><*><00:00-23:55><SURE>

2.检查address字段。可为空，但内容不能为0

3.online字段仍保持为0，不做更改

"""

# encoding=utf8

import dataset

SQL_DICT = {
    'host': '10.10.114.35',
    'user': 'reader',
    'passwd': 'miaoji1109',
    'charset': 'utf8',
    'db': 'rest_merge'
}
table_name = 'chat_restaurant'


def get_online_db_id():
    db = dataset.connect('mysql://reader:miaoji1109@10.10.222.209/base_data?charset=utf8')
    table = db.query('select id from {}'.format(table_name))
    now_set = {line['id'] for line in table}
    target_db = dataset.connect('mysql://hourong:hourong@localhost/data_prepare?charset=utf8')
    target_table = target_db.query('select id from {}'.format(table_name))
    for line in target_table:
        now_set.add(line['id'])
    return now_set


def updatePoiByRaw(online_ids):
    db = dataset.connect('mysql://{user}:{passwd}@{host}/{db}?charset={charset}'.format(**SQL_DICT))
    target_db = dataset.connect('mysql://hourong:hourong@localhost/data_prepare?charset=utf8')
    target_table = target_db[table_name]
    table = db.query(
        'SELECT * FROM {} WHERE id NOT IN ({}) LIMIT 100000'.format(
            table_name,
            ','.join(
                map(lambda x: '"' + x + '"', online_ids)
            )
        )
    )
    rows = []
    _count = 0
    for row in table:
        if row['open_time'].lower() in ['0', 'null', '']:
            row['open_time'] = '<*><*><00:00-23:55><SURE>'

        if row['address'].lower() in ['0', 'null', '']:
            row['address'] = ''

        row['online'] = '0'
        _count += 1
        rows.append(row)
        if _count % 500 == 0:
            print('#' * 100)
            print('Total:', _count)
            target_table.insert_many(rows)
            rows = []
    print('#' * 100)
    print('Total:', _count)
    target_table.insert_many(rows)


if __name__ == '__main__':
    while True:
        online_ids = get_online_db_id()
        if len(online_ids) < 3000000:
            updatePoiByRaw(online_ids)
