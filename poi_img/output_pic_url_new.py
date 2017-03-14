from collections import defaultdict

import db_localhost


def get_task():
    sql = 'select img_list from image_recrawl'
    for line in db_localhost.QueryBySQL(sql):
        for url in line['img_list'].split('|'):
            yield url


def get_task_dict():
    id_img_list_dict = defaultdict(list)
    sql = 'select mid,img_list from image_recrawl'
    for line in db_localhost.QueryBySQL(sql):
        id_img_list_dict[line['mid']].extend(line['img_list'].split('|'))
    return id_img_list_dict


if __name__ == '__main__':
    count = 0
    url_set = set()
    for url in get_task():
        count += 1
        url_set.add(url)
    print(count)
    print((len(url_set)))
