import db_localhost

db = db_localhost


def get_tp_city_info(source_city_id):
    sql = 'select * from tp_all_cities where gid="%s"' % source_city_id
    return db.QueryBySQL(sql)
