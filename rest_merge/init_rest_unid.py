import re

import db_localhost as db

'''
| id           | varchar(64)  | NO   |     | NULL    |       |
| name         | varchar(128) | NO   |     | NULL    |       |
| name_en      | varchar(128) | NO   |     | NULL    |       |
| source       | varchar(128) | NO   | PRI | NULL    |       |
| source_id    | varchar(64)  | NO   | PRI | NULL    |       |
| site         | varchar(320) | NO   |     | NULL    |       |
| city_id      | varchar(128) | NO   | PRI | NULL    |       |
| city_name    | varchar(256) | NO   |     | NULL    |       |
| country_name | varchar(256) | NO   |     | NULL    |       |
'''
d_pattern = re.compile('-d(\d+)')


def insert_db(args):
    sql = 'insert ignore into rest_merge.rest_unid(`id`,`name`,`name_en`,`source`,`source_id`,`site`,`city_id`) values(%s,%s,%s,%s,%s,%s,%s)'
    return db.ExecuteSQLs(sql, args)


def get_task():
    sql = 'select id,name,name_en,res_url,city_id,website_url from poi.restaurant_tmp'
    datas = []
    for line in db.QueryBySQL(sql):
        url = line['res_url']

        miaoji_id = line['id']
        name = line['name']
        name_en = line['name_en']
        source = 'daodao'
        source_id = d_pattern.findall(url)[0]
        site = line['website_url']
        city_id = line['city_id']
        data = (miaoji_id, name, name_en, source, source_id, site, city_id)
        datas.append(data)
    return insert_db(datas)


if __name__ == '__main__':
    print(get_task())
