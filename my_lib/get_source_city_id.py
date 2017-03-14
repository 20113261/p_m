# coding=utf-8
import db_localhost

db = db_localhost


def get_qyer_source_city_id(args):
    """
    获取穷游的城市id
    :param args: tuple(国家名,城市名,城市英文名)
    :return: list
    """
    country_name = args[0]
    city_name = args[1]
    city_name_en = args[2]
    if city_name == '' or city_name is None:
        city_name = 'e9de8493e3af842e3d66aed90d5da984'
    if city_name_en == '' or city_name_en is None:
        city_name_en = 'e9de8493e3af842e3d66aed90d5da984'

    sql = 'select source_city_id from qyer_city where source_country_name="%s" and (source_city_name="%s" or source_city_name_en="%s")' % (
        country_name, city_name, city_name_en)
    res = db.QueryBySQL(sql)
    if len(res) == 0:
        sql = 'select source_city_id from qyer_city where source_country_name="%s" and (source_city_name like "%%%s%%" or source_city_name_en like "%%%s%%")' % (
            country_name, city_name, city_name_en)
        res = db.QueryBySQL(sql)
    # if len(res) == 0:
    #     sql = 'select source_city_id from qyer_city where (source_city_name like "%%%s%%" or source_city_name_en like "%%%s%%")' % (
    #         city_name, city_name_en)
    #     res = db.QueryBySQL(sql)
    return res


def get_tp_source_city_id(args):
    '''
    获取道道source_city_id
    :param args: tuple(国家名,城市名,城市英文名)
    :return: list
    '''
    country_name = args[0]
    city_name = args[1]
    city_name_en = args[2]
    if city_name == '' or city_name is None:
        city_name = 'e9de8493e3af842e3d66aed90d5da984'
    if city_name_en == '' or city_name_en is None:
        city_name_en = 'e9de8493e3af842e3d66aed90d5da984'
    sql = 'select gid from tp_all_cities where country_name="%s" and (city_name = "%s" or city_name="%s" or city_name_alias = "%s" or city_name_alias="%s")' % (
        country_name, city_name, city_name_en, city_name, city_name_en)
    res = db.QueryBySQL(sql)
    if len(res) == 0:
        sql = 'select gid from tp_all_cities where country_name="%s" and (city_name like "%%%s%%" or city_name="%s" or city_name_alias like "%%%s%%" or city_name_alias="%s")' % (
            country_name, city_name, city_name_en, city_name, city_name_en)
        res = db.QueryBySQL(sql)
    # if len(res) == 0:
    #     sql = 'select gid from tp_all_cities where (city_name like "%%%s%%" or city_name LIKE "%%%s%%" or city_name_alias like "%%%s%%" or city_name_alias LIKE "%%%s%%")' % (
    #         city_name, city_name_en, city_name, city_name_en)
    #     res = db.QueryBySQL(sql)
    return res
