from collections import defaultdict

import MySQLdb

ATTR_TABLE = 'tp_attr_basic_1227'
REST_TABLE = 'tp_rest_basic_1227'
SHOP_TABLE = 'tp_shop_basic_1227'


def get_city_count_dict():
    conn = MySQLdb.connect(host='localhost', user='hourong', passwd='hourong',
                           db='hourong', charset="utf8")
    with conn.cursor() as cursor:
        # attr
        sql = 'SELECT city_id,count(*) FROM {0} GROUP BY city_id'.format(ATTR_TABLE)
        cursor.execute(sql)
        attr_count = defaultdict(int)
        for line in cursor.fetchall():
            attr_count[line[0]] = line[1]

        # rest
        sql = 'SELECT city_id,count(*) FROM {0} GROUP BY city_id'.format(REST_TABLE)
        cursor.execute(sql)
        rest_count = defaultdict(int)
        for line in cursor.fetchall():
            rest_count[line[0]] = line[1]

        # shop
        sql = 'SELECT city_id,count(*) FROM {0} GROUP BY city_id'.format(SHOP_TABLE)
        cursor.execute(sql)
        shop_count = defaultdict(int)
        for line in cursor.fetchall():
            shop_count[line[0]] = line[1]

    return attr_count, rest_count, shop_count


def city_ids():
    return iter(
        ["10415", "11534", "11555", "20045", "10423", "10426", "11512", "10427", "10024", "10059", "10158", "10242",
         "10428", "11529", "11530", "11531", "11532", "11552", "11557", "11560", "10443", "10109", "10281", "10300",
         "11322", "11527", "11543", "11548", "10039", "10110", "10123", "10133", "10135", "10140", "10180", "10192",
         "10194", "10408", "10483", "11123", "11266", "11517", "11518", "11541", "11546", "11556", "11564", "10487",
         "10494", "10094", "10147", "10182", "10199", "10226", "10249", "10729", "11338", "11365", "11537", "11554",
         "10141", "10230", "10075", "10540", "11535", "11545", "11550", "11558", "11561", "11563", "10055", "10126",
         "10152", "10222", "10324", "10541", "11211", "11549", "11559", "10549", "10550", "10551", "10553", "11528",
         "10555", "10556", "11516", "10561", "11522", "11523", "11524", "11525", "11096", "10186", "10190", "11544",
         "10402", "11070", "10066", "10080", "10137", "10155", "10187", "10272", "10282", "10303", "10384", "10388",
         "11164", "11413", "11533", "11536", "11547", "11562", "11526", "10598", "10615", "11488", "11519", "11520",
         "11521", "11542", "10058", "10060", "10087", "10129", "10205", "11429", "10314", "11300", "11540", "10067",
         "10069", "10125", "10130", "10150", "10163", "10183", "10208", "10210", "10219", "10221", "10224", "10263",
         "10275", "10356", "10656", "11106", "11269", "11513", "11514", "11515", "11538", "11539", "10040", "10044",
         "10105", "10139", "10162", "10232", "10674", "10680", "10683", "11010", "11398", "11405", "11468", "11551",
         "20393"])


if __name__ == '__main__':
    attr, rest, shop = get_city_count_dict()
    print('Attraction')
    _count = 0
    _city_ids = []
    for city_id in city_ids():
        if attr[city_id] < 20:
            _city_ids.append(city_id)
            _count += 1
    print(', '.join(map(lambda x: '"' + x + '"', _city_ids)))
    print('Attraction Count:', _count)
    print('Restaurant')
    _count = 0
    _city_ids = []
    for city_id in city_ids():
        if rest[city_id] < 20:
            _city_ids.append(city_id)
            _count += 1
    print(', '.join(map(lambda x: '"' + x + '"', _city_ids)))
    print('Restaurant Count:', _count)
    print('Shopping')
    _count = 0
    _city_ids = []
    for city_id in city_ids():
        if shop[city_id] < 20:
            _city_ids.append(city_id)
            _count += 1
    print(', '.join(map(lambda x: '"' + x + '"', _city_ids)))
    print('Shopping Count:', _count)
