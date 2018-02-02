# coding=utf-8
import re
from pymysql.cursors import DictCursor
from norm_tag.lang_convert import tradition2simple
from service_platform_conn_pool import base_data_pool
from toolbox.Common import is_legal
from my_logger import get_logger

logger = get_logger("get_norm_tag")

split_pattern = re.compile('[｜|与和/,，_]')

rest_key_words_dict = {'\u9ece\u5df4\u5ae9\u83dc': '\u9ece\u5df4\u5ae9', '\u79d8\u9c81\u83dc': '\u79d8\u9c81',
                       '\u4e9a\u6d32\u6599\u7406': '\u4e9a\u6d32', '\u745e\u5178\u83dc': '\u745e\u5178',
                       '\u6ce2\u65af\u98ce\u5473': '\u6ce2\u65af', '\u5496\u5561\u9986': '\u5496\u5561\u5385',
                       '\u5df4\u57fa\u65af\u5766\u83dc': '\u5df4\u57fa\u65af\u5766',
                       '\u53e4\u5df4\u83dc': '\u53e4\u5df4', '\u725b\u6392\u9986': '\u725b\u6392\u9910\u5385',
                       '\u8428\u5c14\u74e6\u591a\u83dc': '\u8428\u5c14\u74e6\u591a',
                       '\u82cf\u683c\u5170\u83dc': '\u82cf\u683c\u5170',
                       '\u5c3c\u6cca\u5c14\u83dc': '\u5c3c\u6cca\u5c14',
                       '\u4e9a\u7f8e\u5c3c\u4e9a\u83dc': '\u4e9a\u7f8e\u5c3c\u4e9a',
                       '\u6377\u514b\u83dc': '\u6377\u514b', '\u5fb7\u56fd\u83dc': '\u5fb7\u56fd',
                       '\u6c11\u65cf\u98ce\u5473': '\u6c11\u65cf\u98ce\u5473\u98df\u54c1',
                       '\u7f8e\u56fd\u897f\u5357\u98ce\u5473': '\u4f20\u7edf\u7f8e\u5f0f',
                       '\u5370\u5c3c\u83dc': '\u5370\u5c3c', '\u5404\u56fd\u6599\u7406': '\u56fd\u9645',
                       '\u9152\u5427\u9910': '\u9152\u5427',
                       '\u7d20\u98df\u9009\u9879': '\u7d20\u98df\u4e3b\u4e49\u83dc\u5f0f',
                       '\u5065\u5eb7\u9910\u996e': '\u5065\u5eb7', '\u83f2\u5f8b\u5bbe\u83dc': '\u83f2\u5f8b\u5bbe',
                       '\u5308\u7259\u5229\u83dc': '\u5308\u7259\u5229',
                       '\u963f\u6839\u5ef7\u83dc': '\u963f\u6839\u5ef7',
                       '\u7f8e\u56fd\u65b0\u5965\u5c14\u826f\u98ce\u5473': '\u4f20\u7edf\u7f8e\u5f0f',
                       '\u571f\u8033\u5176\u83dc': '\u571f\u8033\u5176', '\u6cd5\u9910': '\u6cd5\u56fd',
                       '\u4e4c\u514b\u5170\u83dc': '\u4e4c\u514b\u5170', '\u5e0c\u814a\u83dc': '\u5e0c\u814a',
                       '\u963f\u62c9\u4f2f\u98ce\u5473': '\u963f\u62c9\u4f2f', 'Jewish Qua': '\u72b9\u592a',
                       '\u7231\u5c14\u5170\u83dc': '\u7231\u5c14\u5170',
                       '\u4e0d\u542b\u9eb8\u8d28': '\u65e0\u8c37\u86cb\u767d',
                       '\u54e5\u4f26\u6bd4\u4e9a\u83dc': '\u54e5\u4f26\u6bd4\u4e9a',
                       '\u7d20\u98df': '\u7d20\u98df\u4e3b\u4e49\u83dc\u5f0f', '\u6ce2\u5170\u83dc': '\u6ce2\u5170',
                       '\u590f\u5a01\u5937\u83dc': '\u590f\u5a01\u5937',
                       '\u6bd4\u5229\u65f6\u83dc': '\u6bd4\u5229\u65f6', '\u82f1\u56fd\u83dc': '\u82f1\u56fd',
                       '\u52a0\u62ff\u5927\u83dc': '\u52a0\u62ff\u5927',
                       '\u57c3\u585e\u4fc4\u6bd4\u4e9a\u83dc': '\u57c3\u585e\u4fc4\u6bd4\u4e9a',
                       '\u4e2d\u7f8e\u98ce\u5473': '\u4e2d\u7f8e\u83dc', '\u667a\u5229\u83dc': '\u667a\u5229',
                       '\u62c9\u4e01\u98ce\u5473': '\u62c9\u4e01', '\u745e\u58eb\u83dc': '\u745e\u58eb',
                       '\u63d0\u4f9b\u7d20\u98df': '\u7d20\u98df\u4e3b\u4e49\u83dc\u5f0f',
                       '\u5357\u7f8e\u98ce\u5473': '\u5357\u7f8e', '\u8499\u53e4\u83dc': '\u8499\u53e4',
                       '\u57c3\u53ca\u83dc': '\u57c3\u53ca', '\u6469\u6d1b\u54e5\u83dc': '\u6469\u6d1b\u54e5',
                       '\u4ee5\u8272\u5217\u83dc': '\u4ee5\u8272\u5217', '\u72b9\u592a\u6d01\u98df': '\u72b9\u592a',
                       '\u59d4\u5185\u745e\u62c9\u83dc': '\u59d4\u5185\u745e\u62c9',
                       '\u8461\u8404\u7259\u83dc': '\u8461\u8404\u7259', '\u7f05\u7538\u83dc': '\u7f05\u7538',
                       '\u6fb3\u6d32\u83dc': '\u6fb3\u5927\u5229\u4e9a', '\u8377\u5170\u83dc': '\u8377\u5170',
                       '\u975e\u6d32\u6599\u7406': '\u975e\u6d32', '\u897f\u73ed\u7259\u83dc': '\u897f\u73ed\u7259',
                       '\u6c64': '\u6c64\u7c7b', '\u6ce2\u591a\u9ece\u5404\u83dc': '\u6ce2\u591a\u9ece\u5404',
                       '\u65e5\u5f0f\u6599\u7406': '\u65e5\u672c\u6599\u7406',
                       '\u4e2d\u4e1c\u98ce\u5473': '\u4e2d\u4e1c\u6599\u7406',
                       '\u7259\u4e70\u52a0\u83dc': '\u7259\u4e70\u52a0',
                       '\u7f8e\u5f0f\u70f9\u996a': '\u4f20\u7edf\u7f8e\u5f0f',
                       '\u4e2d\u6b27\u98ce\u5473': '\u4e2d\u6b27', '\u5965\u5730\u5229\u83dc': '\u5965\u5730\u5229',
                       '\u9a6c\u6765\u897f\u4e9a\u83dc': '\u9a6c\u6765\u897f\u4e9a',
                       '\u5df4\u897f\u83dc': '\u5df4\u897f',
                       '\u63d0\u4f9b\u65e0\u9eb8\u8d28\u7f8e\u98df': '\u65e0\u8c37\u86cb\u767d',
                       '\u4e39\u9ea6\u83dc': '\u4e39\u9ea6', '\u8857\u8fb9\u5c0f\u5403': '\u5c0f\u5403\u644a',
                       '\u73af\u592a\u5e73\u6d0b\u98ce\u5473': '\u73af\u592a\u5e73\u6d0b',
                       '\u610f\u9910': '\u610f\u5927\u5229'
                       }

tag_dict = {}


def get_tagid_dict(_poi_type):
    _dict = {}
    if _poi_type == 'attr':
        sql = '''SELECT
  tag,
  tag_en,
  original_tag
FROM chat_attraction_tagS
ORDER BY id;'''
    elif _poi_type == 'rest':
        sql = 'select tag,tag_en,original_tag from chat_restaurant_tagS'
    elif _poi_type == 'shop':
        sql = '''SELECT
  tag,
  tag_en,
  original_tag
FROM chat_shopping_tagS
ORDER BY id;'''
    else:
        raise TypeError("Unknown Type: {}".format(_poi_type))

    conn = base_data_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute(sql)
    for line in cursor.fetchall():
        tag = line['tag']
        tag_en = line['tag_en']
        original_tag = line['original_tag']
        _tags_set = set()
        for each_tag in original_tag.split('|'):
            if is_legal(each_tag):
                _tags_set.add(each_tag)
        _dict[tuple(_tags_set)] = (tag, tag_en)
    return _dict


def get_norm_tag(tag_id, _poi_type):
    global tag_dict
    if _poi_type not in tag_dict:
        logger.debug("[init tagid][poi_type: {}]".format(_poi_type))
        tag_dict[_poi_type] = get_tagid_dict(_poi_type)
    norm_tags = []
    norm_tag_ens = []
    unknown = []
    lines = tradition2simple(tag_id).decode()
    for raw_tag in split_pattern.split(lines):
        tag_ok = False
        tag = raw_tag.strip()
        for t_set, values in tag_dict[_poi_type].items():
            if tag in t_set:
                norm_tags.append(values[0])
                norm_tag_ens.append(values[1])
                tag_ok = True
                break
        if not tag_ok:
            if is_legal(tag):
                unknown.append(tag)
    norm_tag = '|'.join(sorted(norm_tags))
    norm_tag_en = '|'.join(sorted(norm_tag_ens))
    return norm_tag, norm_tag_en, unknown
