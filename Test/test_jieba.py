#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/27 下午5:29
# @Author  : Hou Rong
# @Site    : 
# @File    : test_jieba.py
# @Software: PyCharm
# encoding=utf-8
import jieba
import pymongo
import pandas
from collections import defaultdict
from Test.test_split_word import full_split
from Test.test_split_count import word_combination_without_prefix, word_combination

client = pymongo.MongoClient(host='10.10.231.105')
collections = client['CrawlData']['TravelAgency']

min_count_dict = defaultdict(int)
full_count_dict = defaultdict(int)
search_count_dict = defaultdict(int)
full_split_dict = defaultdict(int)
word_combine_dict = defaultdict(int)
word_combine_with_out_prefix_dict = defaultdict(int)

for each in collections.find({}):
    agency_name = each["name"]
    agency_name = agency_name.replace('(', ' ').replace('（', ' ').replace(')', '').replace('）', '')
    min_cut = jieba.cut(agency_name)
    full_cut = jieba.cut(agency_name, cut_all=True)
    search_cut = jieba.cut_for_search(agency_name)
    # print("min cut", agency_name, "->", ", ".join(min_cut))
    # print("full cut", agency_name, "->", ", ".join(full_cut))
    # print("search cut", agency_name, "->", ", ".join(search_cut))
    for k in min_cut:
        min_count_dict[k] += 1
    for k in full_cut:
        full_count_dict[k] += 1
    for k in search_cut:
        search_count_dict[k] += 1
    # for k in full_split(word=agency_name):
    #     full_count_dict[k] += 1
    tmp_min_cut = list(jieba.cut(agency_name))
    # print(tmp_min_cut, list(word_combination(tmp_min_cut)), list(word_combination_without_prefix(tmp_min_cut)))
    for k in word_combination(tmp_min_cut):
        word_combine_dict[k] += 1
    for k in word_combination_without_prefix(tmp_min_cut):
        word_combine_with_out_prefix_dict[k] += 1

# data = []
# for k, v in min_count_dict.items():
#     if k.strip():
#         data.append({
#             'key_words': k,
#             'count': v
#         })
# table = pandas.DataFrame(columns=['key_words', 'count'], data=data)
# table['percent'] = table['count'] / table['count'].sum() * 100
# table.sort_values(by=['count'], ascending=False).head(1000).to_csv('/tmp/min_cut_report.csv')
#
# data = []
# for k, v in full_count_dict.items():
#     if k.strip():
#         data.append({
#             'key_words': k,
#             'count': v
#         })
# table = pandas.DataFrame(columns=['key_words', 'count'], data=data)
# table['percent'] = table['count'] / table['count'].sum() * 100
# table.sort_values(by=['count'], ascending=False).head(1000).to_csv('/tmp/full_cut_report.csv')
#
# data = []
# for k, v in search_count_dict.items():
#     if k.strip():
#         data.append({
#             'key_words': k,
#             'count': v
#         })
# table = pandas.DataFrame(columns=['key_words', 'count'], data=data)
# table['percent'] = table['count'] / table['count'].sum() * 100
# table.sort_values(by=['count'], ascending=False).head(1000).to_csv('/tmp/search_cut_report.csv')
#
# data = []
# for k, v in full_count_dict.items():
#     if k.strip():
#         data.append({
#             'key_words': k,
#             'count': v
#         })
# table = pandas.DataFrame(columns=['key_words', 'count'], data=data)
# table['percent'] = table['count'] / table['count'].sum() * 100
# table.sort_values(by=['count'], ascending=False).head(1000
#                                                       ).to_csv('/tmp/full_split_report.csv')

data = []
for k, v in word_combine_dict.items():
    if k.strip():
        data.append({
            'key_words': k,
            'count': v
        })
table = pandas.DataFrame(columns=['key_words', 'count'], data=data)
table['percent'] = table['count'] / table['count'].sum() * 100
table.sort_values(by=['count'], ascending=False).head(1000
                                                      ).to_csv('/tmp/word_combine_report.csv')

data = []
for k, v in word_combine_with_out_prefix_dict.items():
    if k.strip():
        data.append({
            'key_words': k,
            'count': v
        })
table = pandas.DataFrame(columns=['key_words', 'count'], data=data)
table['percent'] = table['count'] / table['count'].sum() * 100
table.sort_values(by=['count'], ascending=False).head(1000
                                                      ).to_csv('/tmp/word_combine_with_out_prefix_report.csv')
