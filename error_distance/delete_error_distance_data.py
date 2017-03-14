# coding=utf-8
import math

import db_devdb
# import db_localhost as db
from . import db_error_distance as db

TYPE = 'shop'
POI_TABLE = 'chat_shopping'


# lon lat
def get_distance(lon_1, lat_1, lon_2, lat_2):
    """
    从经纬度获取距离
    :param lon_1: 1纬度
    :param lat_1: 1经度
    :param lon_2: 2纬度
    :param lat_2: 2经度
    :return:
    """
    R = 6371.0
    X1 = float(lon_1) * 3.14 / 180
    Y1 = float(lat_1) * 3.14 / 180

    X2 = float(lon_2) * 3.14 / 180
    Y2 = float(lat_2) * 3.14 / 180

    distance = R * math.acos(math.cos(Y1) * math.cos(Y2) * math.cos(X1 - X2) + math.sin(Y1) * math.sin(Y2))
    return distance


def get_city_id_map_info_name_dict():
    city_id_map_info_dict = {}
    city_id_name_dict = {}
    sql = 'select id,name,map_info from city'
    for line in db_devdb.QueryBySQL(sql):
        city_id_map_info_dict[line['id']] = line['map_info']
        city_id_name_dict[line['id']] = line['name']
    return city_id_map_info_dict, city_id_name_dict


def delete_city_id(args):
    sql = 'delete from {0} where id=%s'.format(POI_TABLE)
    return db.ExecuteSQLs(sql, args)


def get_poi_info():
    city_id_map_info_dict, city_id_name_dict = get_city_id_map_info_name_dict()
    sql = 'select city_id,count(*) as total,group_concat(id,"%%%",map_info separator "###") as value from {0} group by city_id'.format(
        POI_TABLE)
    data = []
    for line in db.QueryBySQL(sql):
        city_id = line['city_id']
        values = line['value']
        if city_id not in city_id_map_info_dict:
            continue
        city_map_info = city_id_map_info_dict[city_id]
        try:
            c_lat, c_lon = city_map_info.split(',')
        except:
            continue
        for value in values.split('###'):
            poi_id, poi_map_info = value.replace(' ', '').split('%%%')
            if poi_map_info == '':
                continue
            poi_lat, poi_lon = poi_map_info.split(',')
            distance = get_distance(c_lon, c_lat, poi_lon, poi_lat)
            if distance > 100:
                data.append((poi_id,))
    print(delete_city_id(data))


if __name__ == '__main__':
    get_poi_info()
