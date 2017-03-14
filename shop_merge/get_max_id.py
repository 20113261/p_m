import db_localhost as db


def get_max_id():
    id_set = set()
    for line in db.QueryBySQL('select id from shop_merge.shop_unid'):
        id_set.add(int(line['id'][2:]))
    return 'sh' + str(max(id_set))

if __name__ == '__main__':
    print(get_max_id())