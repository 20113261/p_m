#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/23 下午2:07
# @Author  : Hou Rong
# @Site    : 
# @File    : settings.py
# @Software: PyCharm
PRIVATE_DB_STR = 'mysql+pymysql://reader:miaoji1109@10.10.149.146/private_data?charset=utf8'
TARGET_DB_STR = 'mysql+pymysql://hourong:hourong@10.10.180.145/base_data?charset=utf8'
ID_MAP_DB_STR = 'mysql+pymysql://hourong:hourong@10.10.180.145/IdMap?charset=utf8'

CITY_UNIQUE_KEY = ['id']
ATTR_UNIQUE_KEY = ['id']
REST_UNIQUE_KEY = ['id']
SHOP_UNIQUE_KEY = ['id']

CITY_KEY_MAP = {
    'img_list': 'new_product_city_pic'
}

ATTR_KEY_MAP = {
}

REST_KEY_MAP = {
}

SHOP_KEY_MAP = {
}

CITY_KEYS = [
    'name',
    'name_en',
    'map_info',
    'time_zone',
    'dur',
    'country_id',
    'prov_id',
    'img_list'
]

# data source 指定为 旅游局
ATTR_KEYS = [
    'add_info',
    'address',
    'address_en',
    'alias',
    'beentocount',
    'blong_to_list',
    'city_id',
    'close',
    'comment_info',
    'commentcount',
    'data_source',
    'desc_status',
    'description',
    'description_en',
    'disable',
    'event_mark',
    'first_image',
    'fix_ranking',
    'grade',
    'hot',
    'hot_level',
    'image',
    'image_list',
    'image_num',
    'intensity',
    'intensity_percent',
    'introduction',
    'level',
    'map_info',
    'miojiTag',
    'miojiTag_en',
    'moneyType',
    'name',
    'name_en',
    'name_en_alias',
    'nearCity',
    'norm_price',
    'norm_tagid',
    'norm_tagid_en',
    'open',
    'open_desc',
    'ori_grade',
    'phone',
    'plantocount',
    'prize',
    'ranking',
    'rcmd_intensity',
    'rcmd_open',
    'rcmd_visit_time',
    'real_ranking',
    'recommend_lv',
    'schedule',
    'schedule_en',
    'star',
    'tagB',
    'tagid',
    'ticket',
    'ticket_desc',
    'traveler_choice',
    'url',
    'utime',
    'view_status',
    'visit_time',
    'website_url'
]

REST_KEYS = [
    'address',
    'avg_price',
    'blong_to_list',
    'city_id',
    'comm_bad',
    'comm_good',
    'comment_info',
    'comments',
    'cuisines',
    'description',
    'description_en',
    'dining_options',
    'disable',
    'first_image',
    'fix_ranking',
    'grade',
    'grade_comm',
    'grade_comm_sub',
    'hot',
    'hot_level',
    'icon',
    'icon_file',
    'image_list',
    'image_urls',
    'introduction',
    'level',
    'map_info',
    'max_price',
    'michelin_star',
    'min_price',
    'miojiTag',
    'miojiTag_en',
    'money_type',
    'name',
    'name_en',
    'norm_tagid',
    'norm_tagid_en',
    'open_time',
    'open_time_desc',
    'order_info',
    'payment',
    'phone',
    'price',
    'price_level',
    'prize',
    'ranking',
    'rcmd_open',
    'real_ranking',
    'recommendation',
    'res_url',
    'review_num',
    'service',
    'source',
    'source_id',
    'status',
    'tagB',
    'tagid',
    'tags_comm',
    'telphone',
    'traveler_choice',
    'url',
    'utime',
    'website_url'
]

SHOP_KEYS = [
    'address',
    'beentocount',
    'blong_to_list',
    'city_id',
    'close',
    'comment_info',
    'commentcount',
    'data_source',
    'description',
    'description_en',
    'disable',
    'first_image',
    'fix_ranking',
    'grade',
    'hot',
    'hot_level',
    'image',
    'image_list',
    'intensity',
    'introduction',
    'level',
    'map_info',
    'miojiTag',
    'miojiTag_en',
    'name',
    'name_en',
    'nearCity',
    'norm_tagid',
    'norm_tagid_en',
    'open',
    'open_desc',
    'open_up_to_date',
    'phone',
    'plantocount',
    'prize',
    'ranking',
    'rcmd_intensity',
    'rcmd_open',
    'rcmd_visit_time',
    'real_ranking',
    'recommend_lv',
    'star',
    'tagB',
    'tagid',
    'ticket',
    'ticket_desc',
    'ticket_up_to_date',
    'traveler_choice',
    'url',
    'utime',
    'website_url'
]
