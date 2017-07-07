#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/6 下午4:19
# @Author  : Hou Rong
# @Site    : 
# @File    : ctrip.py
# @Software: PyCharm
import pymongo
import json
import dataset
import Common.MiojiSimilarCityDict
from collections import defaultdict, Counter
from Suggestion.logger import logger_none, logger_multi

Common.MiojiSimilarCityDict.NEED_REGION = True

client = pymongo.MongoClient(host='10.10.231.105')
if __name__ == '__main__':

    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/newspider?charset=utf8')
    target_table = db['hotel_suggestions_city_new']

    mioji_similar_dict = Common.MiojiSimilarCityDict.MiojiSimilarCityDict()

    collections = client['Suggestions']['ctrip']

    # 生成 suggestion List
    city_id_data = defaultdict(list)
    for line in collections.find():
        city_id = line['city_id']
        data = line['data']['data']

        for d in data.split('@'):
            detail_list = d.split('|')
            if len(detail_list) == 13:
                if detail_list[2] == 'city':
                    city_id_data[city_id].append('|'.join(detail_list[:-1]))

    # 获得推荐 Suggestion
    for city_id, suggest_list in city_id_data.items():
        # 初始化变量
        select_index = -1
        suggest_count = Counter(suggest_list)
        new_suggest_list = sorted(suggest_count, key=suggest_count.get, reverse=True)

        # 生成已匹配的 set 用于判断是否存在多匹配
        matched_set = set()

        for each_choice in new_suggest_list:
            # 获取每一条中的城市，国家信息
            each_list = str(each_choice).split('|')
            city_info = each_list[1].split('，')
            if len(city_info) == 2:
                city_name = city_info[0].strip()
                country_name = city_info[-1].strip()
                similar_city_id = mioji_similar_dict.get_mioji_city_id((country_name, city_name))
            elif len(city_info) == 3:
                city_name = city_info[0].strip()
                region_name = city_info[1].strip()
                country_name = city_info[2].strip()
                similar_city_id = mioji_similar_dict.get_mioji_city_id((country_name, region_name, city_name))
            else:
                continue

            # 获得一个相似的城市 id
            if str(similar_city_id) == str(city_id):
                # 或许选中的 suggestion
                select_index = new_suggest_list.index(each_choice) + 1
                matched_set.add(each_choice)

        if len(matched_set) > 1:
            logger_multi.info('#' * 100)
            logger_multi.info(mioji_similar_dict.get_mioji_city_info(city_id) + '\n')
            for j in matched_set:
                logger_multi.info(j)

        if len(matched_set) == 0:
            logger_none.info('#' * 100)
            logger_none.info(mioji_similar_dict.get_mioji_city_info(city_id) + '\n')
            for j in new_suggest_list:
                logger_none.info(j)

        # 插入新的 suggestion 数据
        target_table.insert({
            'city_id': city_id,
            'source': 'ctrip',
            'suggestions': json.dumps(new_suggest_list),
            'select_index': select_index if len(matched_set) == 1 else -1,
            'annotation': '1' if select_index != -1 and len(matched_set) == 1 else '-1',
            'error': '{"code": 0}',
            'label_batch': '20170706a'
        })

    logger_none.info('#' * 100)
    logger_multi.info('#' * 100)
