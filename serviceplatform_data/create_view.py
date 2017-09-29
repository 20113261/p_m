#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/26 下午2:04
# @Author  : Hou Rong
# @Site    : 
# @File    : create_view.py
# @Software: PyCharm
import pymysql
import logging
from logging import getLogger, StreamHandler

logger = getLogger("create_view")
logger.level = logging.DEBUG
s_handler = StreamHandler()
logger.addHandler(s_handler)

if __name__ == '__main__':
    logger.debug("start create view")
    local_conn = pymysql.connect(host='10.10.228.253', user='mioji_admin', charset='utf8', passwd='mioji1109',
                                 db='ServicePlatform')

    logger.debug("get all view name")
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
FROM information_schema.VIEWS
WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    view_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    # drop old view
    local_cursor = local_conn.cursor()
    for v_name in view_list:
        logger.debug("drop view {0}".format(v_name))
        local_cursor.execute('DROP VIEW IF EXISTS {0};'.format(v_name))
    local_cursor.close()

    # get all table name
    local_cursor = local_conn.cursor()
    local_cursor.execute('''SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = 'ServicePlatform';''')
    table_list = list(map(lambda x: x[0], local_cursor.fetchall()))
    local_cursor.close()

    table_name = list(
        filter(lambda x: not x.endswith('test') and x.startswith('detail_'),
               table_list))

    for name in table_name:
        _report_type, _crawl_type, _task_source, _tag_id = name.split('_')

        if _crawl_type == 'hotel':
            detail_name = '_'.join(['detail', _crawl_type, _task_source, _tag_id])
            list_name = '_'.join(['list', _crawl_type, _task_source, _tag_id])
            view_name = '_'.join(['view', _crawl_type, _task_source, _tag_id])
            view_final_name = '_'.join(['view', 'final', _crawl_type, _task_source, _tag_id])

            logger.debug("create view {0}".format(view_name))
            logger.debug("create view {0}".format(view_final_name))

            view_sql = '''DROP VIEW IF EXISTS {0};
        CREATE VIEW {0} AS
          SELECT
            hotel_name,
            hotel_name_en,
            {1}.source,
            {1}.source_id,
            brand_name,
            {1}.map_info,
            address,
            city.name    AS city,
            country.name AS country,
            {2}.city_id,
            postal_code,
            star,
            {1}.grade,
            review_num,
            has_wifi,
            is_wifi_free,
            has_parking,
            is_parking_free,
            service,
            img_items,
            description,
            accepted_cards,
            check_in_time,
            check_out_time,
            {1}.hotel_url,
            update_time,
            continent,
            {2}.country_id
          FROM {1}
            JOIN {2}
              ON {1}.source = {2}.source AND
                 {1}.source_id = {2}.source_id
            JOIN base_data.city ON {2}.city_id = base_data.city.id
            JOIN base_data.country ON base_data.city.country_id = base_data.country.mid;'''.format(
                view_name,
                detail_name,
                list_name)

            view_final_sql = '''DROP VIEW IF EXISTS {0};
CREATE VIEW {0} AS
SELECT
  hotel_name,
  hotel_name_en,
{1}.source,
{1}.source_id,
brand_name,
{1}.map_info,
address,
{2}.city_id,
postal_code,
star,
{1}.grade,
review_num,
has_wifi,
is_wifi_free,
has_parking,
is_parking_free,
service,
img_items,
description,
accepted_cards,
check_in_time,
check_out_time,
{1}.hotel_url,
update_time,
continent,
{2}.country_id
FROM {1}
JOIN {2}
ON {1}.source = {2}.source AND
{1}.source_id = {2}.source_id;'''.format(
                view_final_name,
                detail_name,
                list_name)

            local_cursor = local_conn.cursor()
            local_cursor.execute(view_sql)
            local_cursor.execute(view_final_sql)
            local_conn.commit()
            local_cursor.close()

        elif _crawl_type == 'attr':
            detail_name = '_'.join(['detail', _crawl_type, _task_source, _tag_id])
            list_name = '_'.join(['list', _crawl_type, _task_source, _tag_id])
            view_name = '_'.join(['view', _crawl_type, _task_source, _tag_id])
            view_final_name = '_'.join(['view', 'final', _crawl_type, _task_source, _tag_id])

            logger.debug("create view {0}".format(view_name))

            view_sql = '''DROP VIEW IF EXISTS {0};
CREATE VIEW {0} AS
  SELECT
    {1}.id,
    {1}.source,
    {1}.name,
    {1}.name_en,
    {1}.alias,
    {1}.map_info,
    country.name AS country_name,
    city.name    AS city_name,
    {2}.city_id,
    source_city_id,
    address,
    star,
    recommend_lv,
    pv,
    plantocounts,
    beentocounts,
    overall_rank,
    ranking,
    {1}.grade,
    grade_distrib,
    commentcounts,
    tips,
    tagid,
    related_pois,
    nomissed,
    keyword,
    cateid,
    url,
    phone,
    site,
    imgurl,
    commenturl,
    introduction,
    first_review_id,
    opentime,
    price,
    recommended_time,
    wayto,
    prize,
    traveler_choice,
    {1}.utime
  FROM {1}
    JOIN {2} ON
                                      {1}.source = {2}.source AND
                                      {1}.id = {2}.source_id
    JOIN base_data.city ON base_data.city.id = {2}.city_id
    JOIN base_data.country ON base_data.country.mid = base_data.city.country_id;'''.format(
                view_name,
                detail_name,
                list_name)

            view_final_sql = '''DROP VIEW IF EXISTS {0};
CREATE VIEW {0} AS
SELECT
{1}.id,
{1}.source,
{1}.name,
{1}.name_en,
{1}.alias,
{1}.map_info,
{2}.city_id,
source_city_id,
address,
star,
recommend_lv,
pv,
plantocounts,
beentocounts,
overall_rank,
ranking,
{1}.grade,
grade_distrib,
commentcounts,
tips,
tagid,
related_pois,
nomissed,
keyword,
cateid,
url,
phone,
site,
imgurl,
commenturl,
introduction,
first_review_id,
opentime,
price,
recommended_time,
wayto,
prize,
traveler_choice,
{1}.utime
FROM {1}
JOIN {2} ON
{1}.source = {2}.source AND
{1}.id = {2}.source_id
JOIN base_data.city ON base_data.city.id = {2}.city_id
JOIN base_data.country ON base_data.country.mid = base_data.city.country_id;'''.format(
                view_final_name,
                detail_name,
                list_name)

            local_cursor = local_conn.cursor()
            local_cursor.execute(view_sql)
            local_cursor.execute(view_final_sql)
            local_conn.commit()
            local_cursor.close()

        elif _crawl_type == 'rest':
            detail_name = '_'.join(['detail', _crawl_type, _task_source, _tag_id])
            list_name = '_'.join(['list', _crawl_type, _task_source, _tag_id])
            view_name = '_'.join(['view', _crawl_type, _task_source, _tag_id])
            view_final_name = '_'.join(['view', 'final', _crawl_type, _task_source, _tag_id])

            logger.debug("create view {0}".format(view_name))
            logger.debug("create view {0}".format(view_final_name))

            view_sql = '''DROP VIEW IF EXISTS {0};
CREATE VIEW {0} AS
  SELECT
    {1}.id,
    {1}.source,
    {1}.name,
    {1}.name_en,
    {1}.map_info,
    country.name AS country_name,
    city.name    AS city_name,
    {2}.city_id,
    source_city_id,
    address,
    ranking,
    {1}.grade,
    commentcounts,
    cuisines,
    dining_options,
    payment,
    service,
    level,
    michelin_star,
    recommend,
    rating_by_category,
    menu,
    {1}.status,
    flag,
    url,
    phone,
    site,
    imgurl,
    commenturl,
    introduction,
    first_review_id,
    opentime,
    price,
    price_level,
    prize,
    traveler_choice,
    {1}.utime
  FROM {1}
    JOIN {2} ON {1}.source = {2}.source AND
                                       {1}.id = {2}.source_id
    JOIN base_data.city ON {2}.city_id = base_data.city.id
    JOIN base_data.country ON base_data.city.country_id = base_data.country.mid;'''.format(
                view_name,
                detail_name,
                list_name)

            view_final_sql = '''DROP VIEW IF EXISTS {0};
            CREATE VIEW {0} AS
              SELECT
                {1}.id,
                {1}.source,
                {1}.name,
                {1}.name_en,
                {1}.map_info,
                {2}.city_id,
                source_city_id,
                address,
                ranking,
                {1}.grade,
                commentcounts,
                cuisines,
                dining_options,
                payment,
                service,
                level,
                michelin_star,
                recommend,
                rating_by_category,
                menu,
                {1}.status,
                flag,
                url,
                phone,
                site,
                imgurl,
                commenturl,
                introduction,
                first_review_id,
                opentime,
                price,
                price_level,
                prize,
                traveler_choice,
                {1}.utime
              FROM {1}
                JOIN {2} ON {1}.source = {2}.source AND
                                                   {1}.id = {2}.source_id;'''.format(
                view_final_name,
                detail_name,
                list_name)

            local_cursor = local_conn.cursor()
            local_cursor.execute(view_sql)
            local_conn.commit()
            local_cursor.close()

        elif _crawl_type == 'total':
            detail_name = '_'.join(['detail', _crawl_type, _task_source, _tag_id])
            list_name = '_'.join(['list', _crawl_type, _task_source, _tag_id])
            view_name = '_'.join(['view', _crawl_type, _task_source, _tag_id])
            view_final_name = '_'.join(['view', 'final', _crawl_type, _task_source, _tag_id])

            logger.debug("create view {0}".format(view_name))
            logger.debug("create view {0}".format(view_final_name))

            view_sql = '''DROP VIEW IF EXISTS {0};
CREATE VIEW {0} AS
  SELECT
    {1}.id,
    {1}.source,
    {1}.name,
    {1}.name_en,
    name_py,
    {1}.alias,
    {1}.map_info,
    country.name AS country_name,
    city.name    AS city_name,
    {2}.city_id,
    source_city_id,
    source_city_name,
    source_city_name_en,
    source_country_id,
    source_country_name,
    address,
    star,
    recommend_lv,
    pv,
    plantocounts,
    beentocounts,
    overall_rank,
    ranking,
    {1}.grade,
    grade_distrib,
    commentcounts,
    tips,
    tagid,
    related_pois,
    nomissed,
    keyword,
    cateid,
    url,
    phone,
    site,
    imgurl,
    commenturl,
    introduction,
    opentime,
    price,
    recommended_time,
    wayto,
    crawl_times,
    {1}.status,
    insert_time,
    flag
  FROM {1}
    JOIN list_total_qyer_20170928a ON {1}.source = {2}.source AND
                                      {1}.id = {2}.source_id
    JOIN base_data.city ON base_data.city.id = {2}.city_id
    JOIN base_data.country ON base_data.country.mid = base_data.city.country_id;'''.format(
                view_name,
                detail_name,
                list_name)

            view_final_sql = '''DROP VIEW IF EXISTS {0};
            CREATE VIEW {0} AS
              SELECT
                {1}.id,
                {1}.source,
                {1}.name,
                {1}.name_en,
                name_py,
                {1}.alias,
                {1}.map_info,
                {2}.city_id,
                source_city_id,
                source_city_name,
                source_city_name_en,
                source_country_id,
                source_country_name,
                address,
                star,
                recommend_lv,
                pv,
                plantocounts,
                beentocounts,
                overall_rank,
                ranking,
                {1}.grade,
                grade_distrib,
                commentcounts,
                tips,
                tagid,
                related_pois,
                nomissed,
                keyword,
                cateid,
                url,
                phone,
                site,
                imgurl,
                commenturl,
                introduction,
                opentime,
                price,
                recommended_time,
                wayto,
                crawl_times,
                {1}.status,
                insert_time,
                flag
              FROM {1}
                JOIN {2} ON {1}.source = {2}.source AND
                                                  {1}.id = {2}.source_id;;'''.format(
                view_final_name,
                detail_name,
                list_name)

            local_cursor = local_conn.cursor()
            local_cursor.execute(view_sql)
            local_cursor.execute(view_final_sql)
            local_conn.commit()
            local_cursor.close()
        else:
            logger.debug("could not create view : {0}".format(name))
