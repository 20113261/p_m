import db_localhost

W2N = {
    '¥¥ - ¥¥¥': '23',
    '¥': '1',
    '': '0',
    '¥¥¥¥': '4'
}


def update_db(args):
    sql = 'update data_prepare.restaurant_tmp set price_level=%s where price_level=%s'
    return db_localhost.ExecuteSQLs(sql, args)


if __name__ == '__main__':
    data = []
    for src, dst in W2N.items():
        data.append((dst, src))
    print(update_db(data))
