#coding:utf-8
import csv

from city.config import base_path
from os.path import join as path_join
from os import system
from my_logger import get_logger
import pymysql
from city.config import data_config

poi_and_hotel_report_name = 'poi_and_hotel_report.csv'
merge_image_and_content = 'merge_image_and_content.txt'
logger = get_logger('monitor', base_path)

def make_poi_and_hotel_report(data, param):
    fieldnames = ['任务批次', '抓取源', '抓取类型', '列表页完成', '列表页完成无数据', '列表页无城市数据', '列表页全部', '详情页完成', '详情页完成无数据', '详情页全部', '图片完成', '图片完成无数据', '图片全部']
    csvfile = path_join(base_path, param, poi_and_hotel_report_name)
    with open(csvfile, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for line in data:
            writer.writerow({k:v for k,v in zip(fieldnames, line)})

    logger.info('{}, 生成报表完成'.format(param))
    cmd = "rsync -vI {0} 10.10.150.16::opcity/{1}".format(csvfile, param)
    logger.info('{0}, 上传命令 {1}'.format(param, cmd))
    status = system(cmd)
    logger.info('{0}, 上传返回 {1}'.format(param, str(status)))
    if status==256:
        logger.info('{}, 报表上传成功'.format(param))
    else:
        logger.info('{}, 报表上传失败'.format(param))

    return str(param) +'/'+poi_and_hotel_report_name

def get_content_image_report():
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    sel_content_sql = "select source,count(*) from base_data.hotel_unid group by source;"
    cursor.execute(sel_content_sql)
    contents_title_report = """
source,  total,   success,   failed
"""
    content_report = """{0},  {1},  {2},  {3}"""
    for source, count in cursor.fetchall():
        content = content_report.format(source, count, '100%', '0%') + '\n'
        contents_title_report += content

    sel_image_sql = "select img_list, first_img from base_data.hotel;"
    cursor.execute(sel_image_sql)
    all_count = cursor.rowcount
    gte_10 = 0
    lt_30 = 0
    gt_30 = 0
    zero = 0
    list_and_imgisnull = 0
    for img_list, f_img in cursor.fetchall():
        if img_list in ('', 'NULL', None):
            zero += 1
            continue
        list_total = len(img_list.split('|'))
        if list_total > 30:
            gt_30 += 1
        elif list_total > 10:
            lt_30 += 1
        else:
            gte_10 += 1

        if f_img in ('', 'NULL', None):
            list_and_imgisnull += 1

    img_report = """
酒店总数    无图片    1-10    11-30     >30     无首图
{0}       {1}      {2}    {3}     {4}     {5}
""".format(all_count, zero, gte_10, lt_30, gt_30, list_and_imgisnull)

    cursor.close()
    conn.close()

    return contents_title_report + '\n\n' + img_report

def make_image_content_report(t_all, t_done, t_failed, param):
    txtfile = path_join(base_path, param, merge_image_and_content)
    print(txtfile)

    with open(txtfile, 'w') as f:
        data = """
第 {0} 批
酒店总数 生成成功 生成失败
{1}    {2}     {3}""".format(param, t_all, format(t_done/t_all, '.0%'), format(t_failed/t_all, '.0%'))
        f.write(data+'\n')
        content_and_image = get_content_image_report()
        f.write(content_and_image)
        logger.info('{0}, \n{1}'.format(param, content_and_image))

    cmd = "rsync -vI {0} 10.10.150.16::opcity/{1}".format(txtfile, param)
    logger.info('{0}, 上传命令 {1}'.format(param, cmd))
    status = system(cmd)
    logger.info('{0}, 上传返回 {1}'.format(param, str(status)))
    if status==0:
        logger.info('{}, 报表上传成功'.format(param))
        return True
    else:
        logger.info('{}, 报表上传失败'.format(param))
        return False

def get_file(param, filename):
    cmd = "rsync  10.10.150.16::opcity/{0}/{1} /tmp".format(param, filename)
    status = system(cmd)
    print(status)
    if status==0:
        logger.info('{}, 获取文件成功'.format(param))
        return True
    else:#5888
        logger.info('{}, 获取文件失败'.format(param))
        return False

if __name__ == '__main__':
    make_image_content_report(19666, 19345, 321, '697')
    # make_poi_and_hotel_report('', '674')
    # get_file('674', 'merge_image_and_contenta.txt')