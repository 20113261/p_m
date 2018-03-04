#coding:utf-8
import csv

from city.config import base_path
from os.path import join as path_join
from os import system
from my_logger import get_logger

poi_and_hotel_report_name = 'poi_and_hotel_report.csv'
merge_image_and_content = 'merge_image_and_content.txt'
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

def make_image_content_report(t_all, t_done, t_failed, param):
    txtfile = path_join(base_path, param, merge_image_and_content)
    print(txtfile)
    with open(txtfile, 'w') as f:
        data = '第 {0} 批 总数 {1} 生成成功 {2} 生成失败 {3}'.format(param, t_all, format(t_done/t_all, '.0%'), format(t_failed/t_all, '.0%'))
        f.write(data)
    cmd = "rsync -vI {0} 10.10.150.16::opcity/{1}".format(txtfile, param)
    print(cmd)
    status = system(cmd)
    print(status)
    if status==256:
        logger.info('{}, 报表上传成功'.format(param))
    else:
        logger.info('{}, 报表上传失败'.format(param))

if __name__ == '__main__':
    # make_image_content_report(19666, 19345, 321, '674')
    make_poi_and_hotel_report('', '674')