from collections import defaultdict

import db_114_35_attr
import db_114_35_rest
import db_114_35_shop
import db_localhost


def get_mid_set():
    _set = set()
    if S_TYPE == 'attr':
        sql = 'select DISTINCT miaoji_id from attr_comment_1222 where miaoji_id like "v%"'
    elif S_TYPE == 'rest':
        sql = 'select DISTINCT miaoji_id from attr_comment_1222 where miaoji_id like "r%"'
    elif S_TYPE == 'shop':
        sql = 'select DISTINCT miaoji_id from attr_comment_1222 where miaoji_id like "sh%"'
    else:
        raise TypeError(S_TYPE)
    for line in db_localhost.QueryBySQL(sql=sql):
        _set.add(line['miaoji_id'])
    return _set


def get_city_id_dict():
    _dict = dict()
    if S_TYPE == 'attr':
        sql = 'select id,city_id from chat_attraction'
        db = db_114_35_attr
    elif S_TYPE == 'rest':
        sql = 'select id,city_id from chat_attraction'
        db = db_114_35_rest
    elif S_TYPE == 'shop':
        sql = 'select id,city_id from chat_attraction'
        db = db_114_35_shop
    else:
        raise TypeError(S_TYPE)
    for line in db.QueryBySQL(sql=sql):
        _dict[line['id']] = line['city_id']
    return _dict


if __name__ == '__main__':
    # ---- Variables ----

    S_TYPE = 'attr'

    # -------------------
    city_id_dict = get_city_id_dict()
    mid_set = get_mid_set()

    result = defaultdict(list)
    for mid in mid_set:
        city_id = city_id_dict.get(mid)
        result[city_id].append(mid)

    for k, v in result.items():
        print(k, '-->', v)
    print(len(list(result.keys())))
