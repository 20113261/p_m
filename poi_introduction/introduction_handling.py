import re
import json
import pymysql.cursors

pattern = re.compile(u'[\u4e00-\u9fa5]+')

SQL_DICT = dict(host='10.10.180.145', user='hourong', password='hourong', db='data_prepare', charset='utf8')

if __name__ == '__main__':
    table_name = 'chat_restaurant'
    conn = pymysql.connect(**SQL_DICT)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

    cursor.execute('SELECT id, introduction FROM {}'.format(table_name))
    datas = []

    count = 0
    for row in cursor.fetchall():
        count += 1
        try:
            j_data = json.loads(row['introduction'])
        except Exception:
            j_data = dict()
            j_data['mioji'] = row['introduction']
            print(row['introduction'], row['id'])
        if row['introduction'].lower() in ('0', 'null', ''):
            j_data = dict()

        # 判定 mioji 简介是否符合要求
        if 'mioji' in j_data.keys():
            if j_data['mioji']:
                if len(pattern.findall(j_data['mioji'])) == 0:
                    del j_data['mioji']

        # 判定简介来源
        new_j_data = {}
        if 'mioji' in j_data.keys():
            new_j_data['mioji'] = j_data['mioji']

        if 'qyer' in j_data.keys():
            new_j_data['qyer'] = j_data['qyer']

        introduction = json.dumps(new_j_data)
        res = {
            'introduction': introduction,
            'id': row['id']
        }
        datas.append((introduction, row['id']))
        if count % 10000 == 0:
            cursor.executemany('update {} set introduction=%s where id=%s'.format(table_name), datas)
            datas = []
            print(count)

    cursor.executemany('update {} set introduction=%s where id=%s'.format(table_name), datas)
    datas = []
    print(count)
