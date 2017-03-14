import db_localhost


def get_task():
    sql = 'select map_info,id from shop_merge.shopping_tmp'
    for line in db_localhost.QueryBySQL(sql):
        yield line['map_info'], line['id']


def update_db(args):
    sql = 'update shop_merge.shop_unid set map_info=%s where id=%s'
    return db_localhost.ExecuteSQLs(sql, args)


if __name__ == '__main__':
    data = []
    for map_info, mid in get_task():
        data.append((map_info, mid))
    print(update_db(data))
