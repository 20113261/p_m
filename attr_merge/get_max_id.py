import db_localhost as db


def get_max_id():
    id_set = set()
    for line in db.QueryBySQL('select id from attr_merge.attr_unid'):
        try:
            id_set.add(int(line['id'][1:]))
        except:
            continue
    return 'v' + str(max(id_set))


if __name__ == '__main__':
    print(get_max_id())
