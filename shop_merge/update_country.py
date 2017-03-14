import db_devdb
import db_localhost as db

TASK_TABLE = 'poi.shopping_tmp'

def get_city_id():
    city_id = []
    for line in db.QueryBySQL('select distinct city_id from poi.shopping_tmp'):
        city_id.append(line['city_id'])
    return ','.join(["\"" + x + "\"" for x in city_id])


def get_id_country():
    id_country = []
    sql = 'select id,country from city where id in (%s)' % get_city_id()
    for line in db_devdb.QueryBySQL(sql):
        id_country.append((line['country'], line['id']))
    return id_country


def update_db(args):
    sql = 'update poi.shopping_tmp set country=%s where city_id=%s'
    return db.ExecuteSQLs(sql, args)


if __name__ == '__main__':
    pass