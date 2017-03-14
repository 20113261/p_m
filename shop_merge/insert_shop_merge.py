import json

import db_localhost as db


def get_urls():
    urls = []
    sql = 'select url from poi.shopping_tmp'
    for line in db.QueryBySQL(sql):
        urls.append(json.loads(line['url'])['daodao'])
    return urls


def get_source_id(urls):
    url_source_id = {}
    sql = 'select id,url from tp_shop_basic_0801 where url in (%s)' % ','.join(["\"" + x + "\"" for x in urls])
    for line in db.QueryBySQL(sql):
        url_source_id[line['url']] = line['id']
    return url_source_id


def insert_db(args):
    sql = 'insert into shop_merge.shop_unid (`id`,`name`,`name_en`,`source`,`source_id`,`site`,`city_id`,`city_name`,`country_name`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    return db.ExecuteSQLs(sql, args)


def get_tasks():
    url_source_id = get_source_id(get_urls())
    datas = []
    for line in db.QueryBySQL(
            'select id,name,name_en,city_id,city,country,website_url,url from poi.shopping_tmp'):
        # get source_id by url
        url = json.loads(line['url'])['daodao']
        miaoji_id = line['id']
        name = line['name']
        name_en = line['name_en']
        source = 'daodao'
        source_id = url_source_id[url]
        site = line['website_url']
        city_id = line['city_id']
        city_name = line['city']
        country = line['country']
        data = (miaoji_id, name, name_en, source, source_id, site, city_id, city_name, country)
        datas.append(data)
    print(insert_db(datas))


if __name__ == '__main__':
    get_tasks()
