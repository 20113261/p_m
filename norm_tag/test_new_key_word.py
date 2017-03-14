import csv

import db_localhost as db


def get_id_tag_dict():
    tag_tag_en_dict = {}
    sql = 'select id,tag from tag.restaurant_tagS'
    for line in db.QueryBySQL(sql):
        source_id = line['id']
        tag = line['tag']
        tag_tag_en_dict[source_id] = tag
    return tag_tag_en_dict


id_tag_dict = get_id_tag_dict()
csv_file = open('new_rest_tag.csv')
reader = csv.reader(csv_file)
result = {}
for line in reader:
    if line[0] != '' and line[1] != '' and line[2] == '' and line[3] == '' and line[4] == '':
        result[str(line[0], 'utf8')] = id_tag_dict[int(line[1])]
print(result)
