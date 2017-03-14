import db_localhost as db


def get_source_id_site():
    id_site = {}
    sql = 'select id,site from tp_attr_basic_0801'
    for line in db.QueryBySQL(sql):
        id_site[line['id']] = line['site']
    return id_site


def update_db(args):
    sql = 'update shop_merge.shop_unid set site=%s where source_id=%s'
    return db.ExecuteSQLs(sql, args)


def task():
    id_site = get_source_id_site()
    datas = []
    sql = 'select source_id from shop_merge.shop_unid'
    for line in db.QueryBySQL(sql):
        site = id_site.get(line['source_id'], '')
        data = (site, line['source_id'])
        datas.append(data)
    print(update_db(datas))


if __name__ == '__main__':
    task()
