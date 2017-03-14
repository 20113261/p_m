import db_devdb

db = db_devdb


def get_city_id(args):
    country_name = args[0]
    city_name = args[1]
    city_name_en = args[2]
    if city_name == '':
        city_name = 'e9de8493e3af842e3d66aed90d5da984'
    if city_name_en == '':
        city_name_en = 'e9de8493e3af842e3d66aed90d5da984'
    sql = 'SELECT * FROM `city` where `country`="%s" and (`name`="%s" or `name_en`="%s")' % (
    country_name, city_name, city_name_en)
    res = db.QueryBySQL(sql)
    if len(res) == 0:
        sql = 'SELECT * FROM `city` where `country` like "%%%s%%" and (`name` LIKE "%%%s%%" or `name_en` like "%%%s%%")' % (
        country_name, city_name, city_name_en)
        res = db.QueryBySQL(sql)
    return res
