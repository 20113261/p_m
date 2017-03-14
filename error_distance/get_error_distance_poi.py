# coding=utf-8
import csv
import math

import db_devdb
import db_localhost as db

# import db_error_distance as db

# attr
# TYPE = 'attr'
# POI_TABLE = 'data_prepare.attraction_tmp'

# rest
# TYPE = 'rest'
# POI_TABLE = 'data_prepare.restaurant_tmp'

# shop
TYPE = 'shop'
POI_TABLE = 'data_prepare.shopping_tmp'

DISTANCE = 100


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

    distance = R * math.acos(min(math.cos(Y1) * math.cos(Y2) * math.cos(X1 - X2) + math.sin(Y1) * math.sin(Y2), 1))
    return distance


def get_city_id_map_info_name_dict():
    city_id_map_info_dict = {}
    city_id_name_dict = {}
    sql = 'select id,name,map_info from city'
    for line in db_devdb.QueryBySQL(sql):
        city_id_map_info_dict[line['id']] = line['map_info']
        city_id_name_dict[line['id']] = line['name']
    return city_id_map_info_dict, city_id_name_dict


def get_poi_info():
    writer = csv.writer(open('/tmp/error_distance_poi_{0}.csv'.format(TYPE), 'w'))
    writer_city = csv.writer(open('/tmp/error_distance_city_{0}.csv'.format(TYPE), 'w'))
    writer_reverse = csv.writer(open('/tmp/error_distance_reverse_{0}.csv'.format(TYPE), 'w'))
    writer_city_without_map_info = csv.writer(open('/tmp/city_without_map_info_{0}.csv'.format(TYPE), 'w'))
    writer.writerow(['poi_id', 'poi_lat_lng', 'city_id', 'city_lat_lng', 'distance'])
    writer_city.writerow(['city_id', 'city_name', 'count', 'total', 'percent'])
    writer_reverse.writerow(['poi_id', 'poi_map_info', 'city_id', 'city_map_info'])
    writer_city_without_map_info.writerow(['city_id', 'city_name', 'map_info'])
    city_id_map_info_dict, city_id_name_dict = get_city_id_map_info_name_dict()
    sql = 'select city_id,count(*) as total,group_concat(id,"%%%",map_info separator "###") as value from {0} group by city_id'.format(
        POI_TABLE)
    for line in db.QueryBySQL(sql):
        city_id = line['city_id']
        total = line['total']
        error_count = 0
        values = line['value']
        if city_id not in city_id_map_info_dict:
            continue
        city_map_info = city_id_map_info_dict[city_id]
        city_name = city_id_name_dict[city_id]
        try:
            c_lat, c_lon = city_map_info.split(',')
        except:
            writer_city_without_map_info.writerow([city_id, city_name, city_map_info])
            continue
        for value in values.split('###'):
            poi_id, poi_map_info = value.replace(' ', '').split('%%%')
            if poi_map_info == '':
                continue
            poi_lat, poi_lon = poi_map_info.split(',')
            distance = get_distance(c_lon, c_lat, poi_lon, poi_lat)
            if distance > DISTANCE:
                if get_distance(c_lon, c_lat, poi_lat, poi_lon) < DISTANCE:
                    writer_reverse.writerow([poi_id, poi_map_info, city_id, city_map_info])
                else:
                    writer.writerow([poi_id, ','.join(poi_map_info.split(',')[::-1]), city_id,
                                     ','.join(city_map_info.split(',')[::-1]), distance])
                    error_count += 1
        writer_city.writerow(
            ("%s %s %s %s %.02f%%" % (
                city_id, city_name, error_count, total, 100 * float(error_count) / float(total))).split(
                ' '))


if __name__ == '__main__':
    get_poi_info()
