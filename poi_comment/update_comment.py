from collections import defaultdict

import db_localhost

# import database
import db_114_35_rest as database


def get_task(s_type):
    if s_type == 'attr':
        sql = 'select id,description,description_en from {0}'.format(ATTR_TABLE)
    elif s_type == 'rest':
        sql = 'select id,description,description_en from {0}'.format(REST_TABLE)
    elif s_type == 'shop':
        sql = 'select id,description,description_en from {0}'.format(SHOP_TABLE)
    else:
        raise TypeError()
    for line in database.QueryBySQL(sql):
        yield line['id'], line['description'], line['description_en']


def get_comment_dict():
    comment_cn_dict = defaultdict(list)
    comment_en_dict = defaultdict(list)
    sql = 'select miaoji_id,review_text from Comment.{0} where language="en" and miaoji_id like "r%"'.format(COMMENT_TABLE)
    for line in db_localhost.QueryBySQL(sql):
        comment_en_dict[line['miaoji_id']].append(line['review_text'])
    sql = 'select miaoji_id,review_text from Comment.{0} where language="zhCN" and miaoji_id like "r%"'.format(COMMENT_TABLE)
    for line in db_localhost.QueryBySQL(sql):
        comment_cn_dict[line['miaoji_id']].append(line['review_text'])
    return comment_en_dict, comment_cn_dict


def update_db(args, s_type):
    if s_type == 'attr':
        sql = 'update {0} set description=%s,description_en=%s where id=%s'.format(ATTR_TABLE)
    elif s_type == 'rest':
        sql = 'update {0} set description=%s,description_en=%s where id=%s'.format(REST_TABLE)
    elif s_type == 'shop':
        sql = 'update {0} set description=%s,description_en=%s where id=%s'.format(SHOP_TABLE)
    else:
        raise TypeError()
    return database.ExecuteSQLs(sql, args)


if __name__ == '__main__':
    # ------- variables --------

    S_TYPE = 'rest'
    COMMENT_TABLE = 'tp_comment'
    ATTR_TABLE = 'chat_attraction'
    REST_TABLE = 'chat_restaurant'
    SHOP_TABLE = 'chat_shopping'

    # --------------------------

    comment_en_dict, comment_cn_dict = get_comment_dict()
    data = []
    for mid, desc, desc_en in get_task(S_TYPE):
        comment_cn = '|_|'.join(sorted(comment_cn_dict[mid], key=lambda x: len(x), reverse=True)[:3])
        if not comment_cn:
            comment_cn = desc
        comment_en = '|_|'.join(sorted(comment_en_dict[mid], key=lambda x: len(x), reverse=True)[:3])
        if not comment_en:
            comment_en = desc_en
        data.append((comment_cn, comment_en, mid))
    print(update_db(data, S_TYPE))
