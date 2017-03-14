import json

from . import database


def get_task(s_type):
    if s_type == 'attr':
        sql = 'select id,url from chat_attraction where image_list=""'
    elif s_type == 'rest':
        sql = 'select id,res_url from chat_restaurant where image_list=""'
    elif s_type == 'shop':
        sql = 'select id,url from chat_shopping where image_list=""'
    else:
        raise TypeError()

    if s_type in ['attr', 'shop']:
        for line in database.QueryBySQL(sql):
            url = line['url']
            if 'Attraction_Review' in url:
                for v in list(json.loads(url).values()):
                    if 'Attraction_Review' in str(v):
                        yield line['id'], 'http://www.tripadvisor.cn/' + str(v).split('/', 3)[-1]
    if s_type == 'rest':
        for line in database.QueryBySQL(sql):
            url = line['res_url']
            if 'Restaurant_Review' in url:
                yield line['id'], 'http://www.tripadvisor.cn/' + url.split('/', 3)[-1]


if __name__ == '__main__':
    count = 0
    for mid, url in get_task('attr'):
        print((mid, url))
        count += 1

    print(count)
