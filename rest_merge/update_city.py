import db_devdb
import db_localhost as db


def update_db(name, country, city_id):
    sql = 'update rest_merge.rest_unid set country_name=%s,city_name=%s where city_id=%s'
    return db.ExecuteSQL(sql, (country, name, city_id))


def get_city_info(city_id):
    sql = 'select name,country from city where id=%s' % city_id
    res = db_devdb.QueryBySQL(sql)
    return res[0]['name'], res[0]['country']


def get_task():
    sql = 'select distinct city_id from rest_merge.rest_unid'
    for line in db.QueryBySQL(sql):
        city_id = line['city_id']
        name, country = get_city_info(city_id)
        print(update_db(country, name, city_id))
if __name__ == '__main__':
    get_task()
