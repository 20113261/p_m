#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
from city.config import OpCity_config,base_path, data_config, data_config, ota_config
from collections import defaultdict
from service_platform_report.crawl_data_check_script import detectOriData
import json
import traceback
import sys
import os
from my_logger import get_logger
from city.send_email import send_email
from MongoTask.MongoTaskInsert import InsertTask, TaskType
from call_city_project.step_status import modify_status

param = sys.argv[1]
# param = '671'
# SEND_TO = ['luwanning@mioji.com', 'lidongwei@mioji.com', 'chaisiyuan@mioji.com', 'dujun@mioji.com', 'zhaoxiaoyang@mioji.com']
SEND_TO = ['luwanning@mioji.com', 'cuixiyi@mioji.com']
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step7', path)

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report7=%s,step7=%s where id=%s"
    update_sql_after = "update city_order set step8=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def selectServicePlatform2BaseDataFinal():
    daodao_attr_sql = 'insert into BaseDataFinal.attr_final_{0} select * from ServicePlatform.detail_attr_daodao_{0};'
    qyer_sql = 'insert into BaseDataFinal.total_final_{0} select * from ServicePlatform.detail_total_qyer_{0};'
    select_tag = "select * from Report.service_platform_product_mongo_report where tag like '%%{}'".format(param)
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    try:
        cursor.execute(select_tag, ())
        result = cursor.fetchone()
        if result:
            tag = result[0]
        else:
            raise Exception('没有获取到tag')
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/daodao_attr.sql') as f:
            create_table = f.read()
            create_table = create_table.format(tag)
        cursor.execute(create_table, ())
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/qyer.sql') as f:
            create_table = f.read()
            create_table = create_table.format(tag)
        cursor.execute(create_table, ())

        _daodao_attr_sql = daodao_attr_sql.format(tag)
        cursor.execute(_daodao_attr_sql, ())
        _qyer_sql = qyer_sql.format(tag)
        cursor.execute(_qyer_sql, ())
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception('汇总数据出错: \n{}'.format(traceback.format_exc(e)))
    finally:
        conn.close()

    return tag


def mapping_daodao_by_sid_and_sourcecityid(tag):
    sql = "select sid, city_id from source_info.ota_location where source = 'daodao' and city_id <> 'NULL';"
    conn_ota = pymysql.connect(**ota_config)
    cursor_ota = conn_ota.cursor()
    cursor_ota.execute(sql, ())
    sid_map_city = {sid: city_id for sid, city_id in cursor_ota.fetchall()}
    cursor_ota.close()
    conn_ota.close()

    update_sql = "update BaseDataFinal.attr_final_{0} set city_id=%s where source_city_id=%s".format(tag)
    select_daodao_sql = "select distinct source_city_id from BaseDataFinal.attr_final_{};".format(tag)
    conn_final = pymysql.connect(**data_config)
    cursor_final = conn_final.cursor()
    cursor_final.execute(select_daodao_sql, ())
    for line in cursor_final.fetchall():
        source_city_id = line[0]
        city_id = sid_map_city.get('g'+source_city_id, None)
        if city_id:
            print(source_city_id)
            cursor_final.execute(update_sql, (city_id, source_city_id))
    conn_final.commit()
    cursor_final.close()
    conn_final.close()

def update_mapinfo(tag):
    select_daodao_attr_sql = 'select id, map_info from ServicePlatform.detail_attr_daodao_{0};'.format(tag)
    update_daodao_attr_sql = "update BaseDataFinal.attr_final_{0} set map_info=%s where id=%s".format(tag)
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    cursor.execute(select_daodao_attr_sql)
    for id, map_info in cursor.fetchall():
        if not map_info:
            map_info = 'NULL'
        cursor.execute(update_daodao_attr_sql, (map_info, id))
    conn.commit()

    select_qyer_sql = 'select id, map_info from ServicePlatform.detail_total_qyer_{0} where map_info is not null;'.format(tag)
    update_qyer_sql = "update BaseDataFinal.total_final_{0} set map_info=%s where id=%s".format(tag)
    cursor.execute(select_qyer_sql)
    for id, map_info in cursor.fetchall():
        cursor.execute(update_qyer_sql, (map_info, id))
    conn.commit()

    cursor.close()
    conn.close()

def mapping_daodao_by_othersinfoscityid_and_sourcecityid(tag):
    sql = "select city_id, JSON_EXTRACT(others_info, \"$.s_city_id\") as sid from source_info.ota_location where source = 'qyer' and JSON_EXTRACT(others_info, \"$.s_city_id\")<>'' and city_id<>'NULL';"
    conn_ota = pymysql.connect(**ota_config)
    cursor_ota = conn_ota.cursor()
    cursor_ota.execute(sql, ())
    sid_map_city = {sid[1:-1]: city_id for city_id, sid in cursor_ota.fetchall()}
    cursor_ota.close()
    conn_ota.close()

    update_sql = "update BaseDataFinal.total_final_{0} set city_id=%s where source_city_id=%s".format(tag)
    select_daodao_sql = "select distinct source_city_id from BaseDataFinal.total_final_{};".format(tag)
    conn_final = pymysql.connect(**data_config)
    cursor_final = conn_final.cursor()
    cursor_final.execute(select_daodao_sql, ())
    for line in cursor_final.fetchall():
        source_city_id = line[0]
        city_id = sid_map_city.get(source_city_id, None)
        if city_id:
            print(source_city_id)
            cursor_final.execute(update_sql, (city_id, source_city_id))
    conn_final.commit()
    cursor_final.close()
    conn_final.close()

def check_POI_data(tag):
    qyer_table_name = 'total_final_{}'.format(tag)
    daodao_table_name = 'attr_final_{}'.format(tag)
    qyer_report_result, qyer_tasks_data = detectOriData(qyer_table_name)
    daodao_report_result, daodao_tasks_data = detectOriData(daodao_table_name)
    print(qyer_report_result)
    print(daodao_report_result)
    return qyer_report_result, qyer_tasks_data, daodao_report_result, daodao_tasks_data

def send_tasks(tasks_data, tag):
    source = tasks_data[0]['source']
    task_name = source + '_mapinfo_' + tag
    with InsertTask(worker='proj.total_tasks.supplement_map_info', queue='supplement_field', routine_key='supplement_field',
                    task_name=task_name, source=source.capitalize(), _type='CityInfo',
                    priority=3) as it:
        for line in tasks_data:
            args = line
            it.insert_task(args)

        return it.generate_collection_name(), task_name

def analysis_result(result_report, source):
    i = 0
    report = source + ' '
    for line in result_report:
        i += 1
        report += line['error_type'] + ': ' + str(line['num']) + '  '

    return False if i>2 else True, report+'\n'

def success_report(tag):
    success_rate_sql = "select * from Report.service_platform_product_mongo_report where tag = '{}' and source in ('Daodao', 'Qyer') order by date limit 2;".format(tag)
    data_quality_sql = "select * from Report.service_platform_crawl_error_report where tag='{}' and source in ('daodao', 'qyer') order by date limit 2;".format(tag)
    attr_coverage_sql = "select * from Report.service_platform_attr_data_coverage_report where tag='{}' and source ='daodao' limit 1;".format(tag)
    qyer_coverage_sql = "select * from Report.service_platform_total_data_coverage_report where tag='{}' and source ='qyer' limit 1;".format(tag)

    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()

    cursor.execute(success_rate_sql, ())
    all_finaled_data = cursor.fetchall()

    success_rate_report = '抓取成功率：\n'
    for (_0, source, _2, l_done, l_failed, _5, l_all, d_done, d_failed, d_all, i_done, i_failed, i_all, _13) in all_finaled_data:
        success_rate_report += '   ' + source.lower() + '  列表页成功率: {0}  详情页成功率: {1}  图片成功率: {2}\n'.format(
            format((l_done + l_failed) / l_all, '.0%'),
            format((d_done + d_failed) / d_all, '.0%'),
            format((i_done + i_failed) / i_all, '.0%') if i_all>0 else '无增量图片抓取')

    cursor.execute(data_quality_sql, ())
    all_quality_data = cursor.fetchall()
    all_quality_report = '数据质量统计: \n'
    for (_a, source, _c, total, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m, _n) in all_quality_data:
        all_quality_report += '   ' + source.lower() + '  总数: {8}  数据源错误: {0}  无 name、name_en: {1}  中英文名相反: {2}  中文名含有英文名: {3}  经纬度重复: {4}  坐标与所属城市距离过远: {5}  距离过远坐标翻转后属于所属城市: {6}  静态评分异常(评分高于10分): {7}\n'.format(
            format(_d / total, '.0%'),
            format(_e / total, '.0%'),
            format(_f / total, '.0%'),
            format(_g / total, '.0%'),
            format(_h / total, '.0%'),
            format(_k / total, '.0%'),
            format(_l / total, '.0%'),
            format(_m / total, '.0%'),
            total)

    cursor.execute(attr_coverage_sql, ())
    attr_coverage_data = cursor.fetchall()
    attr_coverage_report = '景点字段覆盖率统计: \n'
    for (_a, source, total, name, name_en, map_info, address, star, grade, ranking, commentcounts, tagid, imgurl, introduction, phone, site, opentime, _1) in attr_coverage_data:
        attr_coverage_report += '   ' + source.lower() + '  总数: {14}  name: {0}  name_en: {1}  map_info: {2}  address: {3}  star: {4}  grade: {5}  ranking: {6}  commentcounts: {7}  tagid: {8}  imgurl: {9}  introduction: {10}  phone: {11}  site: {12} opentime: {13}\n'.format(
            format(name / total, '.0%'),
            format(name_en / total, '.0%'),
            format(map_info / total, '.0%'),
            format(address / total, '.0%'),
            format(star / total, '.0%'),
            format(grade / total, '.0%'),
            format(ranking / total, '.0%'),
            format(commentcounts / total, '.0%'),
            format(tagid / total, '.0%'),
            format(imgurl / total, '.0%'),
            format(introduction / total, '.0%'),
            format(phone / total, '.0%'),
            format(site / total, '.0%'),
            format(opentime / total, '.0%'),
            total)

    cursor.execute(qyer_coverage_sql, ())
    qyer_coverage_data = cursor.fetchall()
    qyer_coverage_report = '穷游字段覆盖率统计: \n'
    for (_a, source, total, name, name_en, map_info, address, star, grade, ranking, beentocounts, plantcount, commentcounts,
         tagid, imgurl, introduction, phone, site, opentime, _1) in qyer_coverage_data:
        qyer_coverage_report += '   ' + source.lower() + '  总数: {16}  name: {0}  name_en: {1}  map_info: {2}  address: {3}  star: {4}  grade: {5}  ranking: {6}  beentocounts: {7}  plantcount: {8}  commentcounts: {9}  tagid: {10} imgurl: {11}  introduction: {12}  phone: {13}  site: {14} opentime: {15}\n'.format(
            format(name / total, '.0%'),
            format(name_en / total, '.0%'),
            format(map_info / total, '.0%'),
            format(address / total, '.0%'),
            format(star / total, '.0%'),
            format(grade / total, '.0%'),
            format(ranking / total, '.0%'),
            format(beentocounts / total, '.0%'),
            format(plantcount / total, '.0%'),
            format(commentcounts / total, '.0%'),
            format(tagid / total, '.0%'),
            format(imgurl / total, '.0%'),
            format(introduction / total, '.0%'),
            format(phone / total, '.0%'),
            format(site / total, '.0%'),
            format(opentime / total, '.0%'),
            total)

    print(success_rate_report)
    print(all_quality_report)
    print(attr_coverage_report)
    print(qyer_coverage_report)
    return success_rate_report+all_quality_report+attr_coverage_report+qyer_coverage_report

def dumps_sql(tag, source):
    prefix = 'total_final_' if source=='total' else 'attr_final_'
    cmd = """mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 BaseDataFinal {0}{1} > /data/hourong/output/{0}{1}.sql"""
    cmd = cmd.format(prefix, tag)
    status = os.system(cmd)
    if status==0:
        return '10.10.114.35::output/{0}{1}.sql'.format(prefix, tag)


def send_email_format(report, rsync_path):
    send_email('城市上线POI融合' + '第 %s 批次' % param, """
hi all：
    {0}
    数据地址： {1}
            """.format(report, rsync_path), SEND_TO)

def task_start():
    logger.info('[step7][%s]======== start =======' % (param,))
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        return_result = json.dumps(return_result)

        logger.info('[step6][%s] 汇总数据到BaseDataFinal 开始' % (param,))
        tag = selectServicePlatform2BaseDataFinal()
        logger.info('[step6][%s]  汇总数据到BaseDataFinal 完成' % (param,))
        logger.info('[step6][%s] mapping daodao 开始' % (param,))
        mapping_daodao_by_sid_and_sourcecityid(tag)
        logger.info('[step6][%s] mapping daodao 开始' % (param,))
        logger.info('[step6][%s] mapping qyer 开始' % (param,))
        mapping_daodao_by_othersinfoscityid_and_sourcecityid(tag)
        logger.info('[step6][%s] mapping qyer 开始' % (param,))
        logger.info('[step6][%s] 检查数据 开始' % (param,))
        _1, qyer_tasks_data, _2, daodao_tasks_data = check_POI_data(tag)
        logger.info('[step6][%s] 检查数据 完成' % (param,))

        tasks_names = []
        if qyer_tasks_data:
            logger.info('[step6][%s] qyer补充mapinfo任务 开始' % (param,))
            collection_name, task_name = send_tasks(qyer_tasks_data, tag)
            tasks_names.append([collection_name, task_name])
            logger.info('[step6][%s] qyer补充mapinfo任务 完成' % (param,))
        if daodao_tasks_data:
            logger.info('[step6][%s] daodao补充mapinfo任务 完成' % (param,))
            collection_name, task_name = send_tasks(daodao_tasks_data, tag)
            tasks_names.append([collection_name, task_name])
        logger.info('[step6][%s] daodao补充mapinfo任务 完成' % (param,))

        # logger.info('[step6][%s] 导出数据 开始' % (param,))
        # data_path = dumps_sql(tag)
        # logger.info('[step6][%s] 导出数据 完成' % (param,))


        tasks = modify_status('step7', param, tasks_names)
        logger.info('[step7][%s] tasks: %s' % (param, str(tasks)))
        # update_step_report('', param, 1, 0)
        logger.info('[step7][%s]======== success =======' % (param,))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        send_email('城市上线酒店融合' + '第 %s 批次' % param,
                   """POI融合前检查失败""", SEND_TO[:1])
        update_step_report('', param, -1, 0)
        logger.info('[step7][%s]======== failed =======' % (return_result,))


if __name__ == "__main__":
    task_start()