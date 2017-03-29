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
  city.alias      AS city_alias,
  country.name_en AS country_name_en,
  country.alias   AS country_alias
FROM city
  INNER JOIN country ON city.country_id = country.mid;''')
        for line in cursor.fetchall():
            city_keys = []
            if line[2] is not None and line[2] != 'NULL' and line[2] != '':
                city_keys.append(line[2])
            if line[3] is not None:
                for c_k in line[3].split('|'):
                    if c_k != 'NULL' and c_k != '':
                        city_keys.append(c_k)

            country_keys = []
            if line[4] is not None and line[4] != 'NULL' and line[4] != '':
                country_keys.append(line[4])
            if line[5] is not None:
                for c_k in line[5].split('|'):
                    country_keys.append(c_k)

            for country in country_keys:
                for city in city_keys:
                    __city_dict[city, country] = [line[0], line[1], line[2], line[4]]

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
                 'ID', 'name', 'name_en', 'country', 'total_ratio'])

    insert_res = result.T
    _line_count = 0
    _count = 0

    for k, v in airport_dict.items():
        _count += 1
        if _count % 100 == 0:
            print(_count)
        wiki_city, wiki_country = k
        is_first = True
        similar_list = []
        for key, value in city_dict.items():
            mioji_city, mioji_country = key

            # 相似判断
            try:
                city_ratio = ratio(wiki_city, mioji_city)
                country_ratio = ratio(wiki_country, mioji_country)
                bool_test = city_ratio > 0.8 and country_ratio > 0.6
                total_ratio = city_ratio + country_ratio
            except:
                bool_test = False
                total_ratio = 0

            if bool_test:
                similar_list.append((value, total_ratio))

        similar_list.sort(key=lambda x: x[1], reverse=True)
        if len(similar_list) == 0:
            res = []
            res.extend(v)
            res.extend([''] * 5)
            insert_res[_line_count] = res
            _line_count += 1
        else:
            id_set = set()
            for similar in similar_list:
                if similar[0][0] not in id_set:
                    res = []
                    if is_first:
                        res.extend(v)
                        is_first = False
                    else:
                        res.extend([''] * 8)
                    res.extend(similar[0])
                    res.append(similar[1])
                    id_set.add(similar[0][0])
                    insert_res[_line_count] = res
                    _line_count += 1

    result_final = insert_res.T
    result_final.to_csv('/Users/hourong/Downloads/test123.csv', encoding="utf8")
