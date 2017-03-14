import pymysql

__test_sql_dict = {
    'host': '10.10.111.62',
    'user': 'reader',
    'passwd': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

__online_sql_dict = {
    'host': '10.10.222.209',
    'user': 'reader',
    'passwd': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


def get_cid_dict():
    __dict = dict()
    conn = pymysql.connect(**__test_sql_dict)
    with conn as cursor:
        cursor.execute('SELECT id, name, name_en, country FROM `city` where newProduct_status in ("Open", "Ready")')
        for mid, name, name_en, country in cursor.fetchall():
            __dict[mid] = (name, name_en, country)
    return __dict


def get_ok_cid_set():
    __set = set()
    conn = pymysql.connect(**__online_sql_dict)
    with conn as cursor:
        cursor.execute('SELECT DISTINCT city_id FROM `chat_restaurant` WHERE online=1')
        for line in cursor.fetchall():
            __set.add(line[0])
    return __set


if __name__ == '__main__':
    f = open('cid_file_0303', 'w')
    cid_dict = get_cid_dict()
    # print(len(cid_dict))
    ok_cid = get_ok_cid_set()
    # print(len(ok_cid))

    ready_id_keys = cid_dict.keys() - ok_cid
    for cid in ready_id_keys:
        data = [cid]
        cid_data = cid_dict[cid]
        data.append(cid_data[0])
        data.append(cid_data[1])
        print(data)
        f.write('\t'.join(data) + '\n')
