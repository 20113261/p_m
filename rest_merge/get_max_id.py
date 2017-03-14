import db_localhost as db


def get_max_id():
    id_set = set()
    for line in db.QueryBySQL('select id from rest_merge.rest_unid'):
        id_set.add(int(line['id'][1:]))
    return 'r' + str(max(id_set))

if __name__ == '__main__':
    print(get_max_id())