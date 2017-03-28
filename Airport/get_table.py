import pandas
import pymysql
from collections import defaultdict
from Levenshtein import ratio

SQL_DICT = {
    'user': 'root',
    'password': '1234',
    'charset': 'utf8',
    'db': 'AirPort'
}

airport_result_dict = defaultdict(list)


def make_map_info(MapInfo):
    lat, lng = MapInfo.split()
    lat = float(lat[:-2]) * (1 if '°N' in lat else -1)
    lng = float(lng[:-2]) * (1 if '°E' in lng else -1)
    return '{},{}'.format(lng, lat)


def get_airport_dict():
    conn = pymysql.connect(**SQL_DICT)
    __airport_dict = {}
    with conn as cursor:
        cursor.execute('''SELECT
      IATA.IATA,
      ICAO,
      `Airport name`,
      `Location served`,
      substring_index(`Location served`, ',', 1)  AS wiki_city_name,
      substring_index(`Location served`, ',', -1)  AS wiki_country_name,
      MapInfo
    FROM IATA
      INNER JOIN IATADetail ON IATA.IATA = IATADetail.IATA''')
        for line in cursor.fetchall():
            __l = list(line)
            __l.append(make_map_info(line[-1]))
            __airport_dict[(line[4], line[5])] = __l
    conn.close()
    return __airport_dict


def get_city_dict():
    conn = pymysql.connect(**SQL_DICT)
    __city_dict = {}
    with conn as cursor:
        cursor.execute('''SELECT
  city.id         AS ID,
  city.name       AS city,
  city.name_en    AS city_name_en,
  country.name_en AS country_name_en
FROM city
  INNER JOIN country ON city.country_id = country.mid;''')
        for line in cursor.fetchall():
            __city_dict[(line[2], line[3])] = line
    conn.close()
    return __city_dict


if __name__ == '__main__':
    city_dict = get_city_dict()
    print('City Ready')
    airport_dict = get_airport_dict()
    print('AirPort Ready')

    result = pandas.DataFrame(
        columns=['IATA', 'ICAO', 'AirPort name', 'Location served', 'wiki city name', 'wiki country name', 'MapInfo',
                 'map_info',
                 'ID', 'name', 'name_en', 'country'])

    insert_res = result.T
    _line_count = 0
    _count = 0

    for k, v in airport_dict.items():
        _count += 1
        if _count % 100 == 0:
            print(_count)
        wiki_city, wiki_country = k
        is_first = True
        for key, value in city_dict.items():
            mioji_city, mioji_country = key

            # 相似判断
            try:
                bool_test = ratio(wiki_city, mioji_city) > 0.8 and ratio(wiki_country, mioji_country) > 0.6
            except:
                bool_test = False

            if bool_test:
                if is_first:
                    is_first = False
                    res = []
                    res.extend(v)
                    res.extend(value)
                    insert_res[_line_count] = res
                    _line_count += 1
                else:
                    res = []
                    res.extend(['' for i in range(len(v))])
                    res.extend(value)
                    insert_res[_line_count] = res
                    _line_count += 1

        if is_first:
            res = []
            res.extend(v)
            res.extend(['' for i in range(4)])
            insert_res[_line_count] = res
            _line_count += 1

    result_final = insert_res.T
    result_final.to_csv('/Users/hourong/Downloads/test123.csv', encoding="utf8")
