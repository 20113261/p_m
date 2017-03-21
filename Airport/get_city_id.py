import dataset
from collections import defaultdict


def get_airport_city_id():
    db = dataset.connect('mysql+pymysql://writer:miaoji1109@10.10.154.38/test?charset=utf8')
    table_airport = db.query("""SELECT
      id,
      map_info
    FROM airport
    WHERE city_id = 'NULL' AND map_info != 'NULL'""")
    table_city = db.query("""SELECT
      id,
      border_map_1,
      border_map_2
    FROM city;""")

    city_list_dict = defaultdict(list)

    city_dict = dict()
    for line in table_city:
        try:
            lon_1, lat_1 = line['border_map_1'].split(',')
            lon_2, lat_2 = line['border_map_2'].split(',')
            if (abs(lon_2 - lon_1) + abs(lat_2 - lat_1)) <= (abs(lon_2 - lat_1) + abs(lat_2 - lon_1)):
                city_dict[line['id']] = (float(lon_1), float(lon_2), float(lat_1), float(lat_2))
            else:
                city_dict[line['id']] = (float(lat_1), float(lon_2), float(lon_1), float(lat_2))
        except:
            continue

    for line in table_airport:
        lon, lat = line['map_info'].split(',')
        try:
            lon = float(lon)
            lat = float(lat)
        except:
            continue
        for k, v in city_dict.items():
            cid = k
            lon_1, lon_2, lat_1, lat_2 = v
            if lon_1 <= lon <= lon_2 and lat_2 <= lat <= lat_1:
                city_list_dict[line['id']].append(cid)

    for k, v in city_list_dict.items():
        print(k, v)
