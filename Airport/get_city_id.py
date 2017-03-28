import dataset
import pandas
from collections import defaultdict


def get_airport_city_id():
    db = dataset.connect('mysql+pymysql://writer:miaoji1109@10.10.154.38/test?charset=utf8')
    table_airport = db.query("""SELECT
          id,
          iata_code,
          name,
          name_en,
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
    airport_dict = dict()
    for line in table_city:
        try:
            lon_1, lat_1 = line['border_map_1'].split(',')
            lon_2, lat_2 = line['border_map_2'].split(',')
            lon_1, lat_1 = float(lon_1), float(lat_1)
            lon_2, lat_2 = float(lon_2), float(lat_2)
            if (abs(lon_2 - lon_1) + abs(lat_2 - lat_1)) <= (abs(lon_2 - lat_1) + abs(lat_2 - lon_1)):
                city_dict[line['id']] = (float(lon_1), float(lon_2), float(lat_1), float(lat_2))
            else:
                city_dict[line['id']] = (float(lat_1), float(lon_2), float(lon_1), float(lat_2))
        except:
            continue

    for line in table_airport:
        airport_dict[line['id']] = (line['name'], line['name_en'], line['iata_code'], line['map_info'])
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

    k_list = []
    v_list = []
    for k, v in city_list_dict.items():
        k_list.append(k)
        name, name_en, iata_code, map_info = airport_dict[k]
        print(k, iata_code, name, name_en, v)
        v_list.append((k, iata_code, name, name_en, map_info, v))

    df = pandas.DataFrame(v_list, columns=['airport_id', 'iata_code', 'name', 'name_en', 'map_info', 'city_id_list'])
    df.to_excel('/tmp/airport_checked.xlsx')
    print(k_list)


if __name__ == '__main__':
    get_airport_city_id()
