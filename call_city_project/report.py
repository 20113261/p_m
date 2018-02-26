#coding:utf-8
import csv

from city.config import base_path
from os.path import join as path_join
from os import system
from my_logger import get_logger

poi_and_hotel_report_name = 'poi_and_hotel_report.csv'
logger = get_logger('monitor', base_path)

def make_poi_and_hotel_report(data, param):
    fieldnames = ['任务批次', '抓取源', '抓取类型', '列表页完成', '列表页完成无数据', '列表页无城市数据', '列表页全部', '详情页完成', '详情页完成无数据', '详情页全部', '图片完成', '图片完成无数据', '图片全部']
    csvfile = path_join(base_path, param, poi_and_hotel_report_name)
    with open(csvfile, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for line in data:
            writer.writerow({k:v for k,v in zip(fieldnames, line)})

    logger.info('{}, 生成报表完成'.format(param))
    cmd = "rsync -vI {0} 10.10.150.16::opcity/{1}".format(csvfile, param)
    status = system(cmd)
    if status==256:
        logger.info('{}, 报表上传成功'.format(param))
    else:
        logger.info('{}, 报表上传失败'.format(param))