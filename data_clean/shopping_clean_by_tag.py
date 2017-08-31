# coding=utf-8
import pymysql
from pymysql.cursors import DictCursor
from Config.settings import shop_merge_conf

'''
1.当某POI仅有购物TAG时，该POI为购物点。
【购物TAG：礼品与特产商店、大型购物中心、农贸市场、跳蚤市场与街边市场、古董店、百货商场、厂家直营店、购物】
ex:老佛爷百货公司：TAG为百货商场|购物→购物点
2.当某POI同时有购物TAG和景点TAG时，只有当该POI的TAG中包含【礼品与特产商店、大型购物中心、百货商场、厂家直营店】之一时，该POI为购物点；其他情况，该POI为景点。
ex:莎士比亚书局：TAG为图书馆|礼品与特产商店|购物|旅客资源|景点与地标→购物点；
    卡地亚当代艺术基金会：TAG为美术馆|博物馆|购物→景点

'''

TASK_TABLE = 'chat_shopping_new'


def get_error_tag_name():
    shopping_tag = ['礼品与特产商店', '大型购物中心', '农贸市场', '跳蚤市场与街边市场', '古董店', '百货商场', '厂家直营店', '购物']
    important_shopping_tag = ['礼品与特产商店', '大型购物中心', '百货商场', '厂家直营店', '购物']
    error_tag_set = set()
    sql = 'select id,norm_tagid from {0}'.format(TASK_TABLE)
    conn = pymysql.connect(**shop_merge_conf)
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)

    for line in cursor.fetchall():
        miaoji_id = line['id']
        tagid = line['norm_tagid']
        tag_list = tagid.split('|')
        if not all([tag.strip() in shopping_tag for tag in tag_list]):
            if not any([tag.strip() in important_shopping_tag for tag in tag_list]):
                error_tag_set.add((miaoji_id,))
    return error_tag_set


def delete_db(miaoji_id_set):
    conn = pymysql.connect(**shop_merge_conf)
    cursor = conn.cursor()
    sql = 'delete from {0} where id=%s'.format(TASK_TABLE)
    res = cursor.executemany(sql, miaoji_id_set)
    conn.close()
    return res


if __name__ == '__main__':
    error_shopping_id = get_error_tag_name()
    count = 0
    for miaoji_id in error_shopping_id:
        print(miaoji_id)
        count += 1
        if count > 100:
            break
    print(len(error_shopping_id))
    print(delete_db(error_shopping_id))
    # print len(error_shopping_id)
    # error_list = []
    # for line in get_tag_name(error_shopping_id):
    #     error_list.append(json.loads(line['tagid'])['daodao'])

    # print len(error_list)
    # for i in set(error_list):
    #     print i
