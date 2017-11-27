#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/24 上午10:22
# @Author  : Hou Rong
# @Site    : 
# @File    : update_hotel_validation.py
# @Software: PyCharm
import pymysql
import datetime
from warnings import filterwarnings
from urllib.parse import urlparse, urljoin
from data_source import MysqlSource
from logger import get_logger, func_time_logger
from service_platform_conn_pool import verify_info_pool
from Common.Utils import retry

filterwarnings('ignore', category=pymysql.err.Warning)
logger = get_logger("update_hotel_validation")

test_db = {
    'host': '10.10.69.170',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}

online_db = {
    'host': '10.10.68.103',
    'user': 'reader',
    'password': 'miaoji1109',
    'charset': 'utf8',
    'db': 'base_data'
}


class UpdateHotelValidation(object):
    def __init__(self, need_load_offset=True):
        self.table_can_be_used = True
        if need_load_offset:
            self.__offset = self.load_offset()
        else:
            self.__offset = 0

        self.pre_offset = self.offset

    @property
    def offset(self):
        return self.__offset

    @offset.setter
    def offset(self, val):
        self.__offset = val
        self.save_offset()

    @staticmethod
    def load_offset():
        conn = verify_info_pool.connection()
        cursor = conn.cursor()
        task_name = datetime.datetime.now().strftime('update-hotel-validation-%Y-%m-%d')
        cursor.execute('''SELECT
  sequence
FROM task_seek
WHERE task_name = '{}';'''.format(task_name))
        _res = cursor.fetchone()
        cursor.close()
        conn.close()
        if _res:
            if _res[0]:
                offset = int(_res[0])
            else:
                offset = 0
        else:
            offset = 0
        logger.info("[get offset][offset: {}]".format(offset))
        return offset

    def save_offset(self):
        conn = verify_info_pool.connection()
        cursor = conn.cursor()
        task_name = datetime.datetime.now().strftime('update-hotel-validation-%Y-%m-%d')
        res = cursor.execute(
            '''REPLACE INTO task_seek (`task_name`, `sequence`) VALUES (%s, %s);''', (task_name, self.offset))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("[save offset][task_name: {}][offset: {}][status: {}]".format(task_name, self.offset, bool(res)))

    @staticmethod
    def default_api_task_key_and_content(each_data):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        content = '{}&{}&'.format(each_data["mid"], each_data["sid"])
        return workload_key, content, workload_source

    @staticmethod
    def sid_only_key_and_content(each_data, double_key=False):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        if not double_key:
            content = '{}&'.format(each_data["sid"])
        else:
            content = '{}&&'.format(each_data["sid"])
        return workload_key, content, workload_source

    @staticmethod
    def booking(each_data):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        # 1231656&hotel/us/victorian-alamo-square-two-bedroom-apartment&
        tmp_url = urlparse(each_data['hotel_url']).path[1:]
        content = '{}&{}&'.format(each_data["sid"], tmp_url.split('.zh-cn.html')[0])
        return workload_key, content, workload_source

    @staticmethod
    def elong(each_data):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        # NULL&&294945&&圣彼得无限酒店&&NULL&&
        content = 'NULL&&{}&&{}&&NULL&&'.format(each_data["sid"], each_data["name"])
        return workload_key, content, workload_source

    @staticmethod
    def expedia(each_data, source='expedia'):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        # http://www.expedia.com.hk/cn/Sapporo-Hotels-La'gent-Stay-Sapporo-Oodori-Hokkaido.h15110395.Hotel-Information?&
        if source == 'expedia':
            host = "https://www.expedia.com.hk/cn"
        elif source == 'ebookers':
            host = "https://www.ebookers.com"
        elif source == 'orbitz':
            host = "https://www.orbitz.com"
        elif source == 'travelocity':
            host = "https://www.travelocity.com"
        elif source == 'cheaptickets':
            host = "https://www.cheaptickets.com"
        else:
            host = "https://www.expedia.com.hk/cn"
        content = "{}{}{}".format(host, urlparse(each_data['hotel_url']).path, '?&')
        return workload_key, content, workload_source

    @staticmethod
    def agoda(each_data):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        # http://www.expedia.com.hk/cn/Sapporo-Hotels-La'gent-Stay-Sapporo-Oodori-Hokkaido.h15110395.Hotel-Information?&
        content = urljoin("https://www.agoda.com", urlparse(each_data['hotel_url']).path) + '&'
        return workload_key, content, workload_source

    @staticmethod
    def hotels(each_data):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        # 洛杉矶&&美国&&1439028&&163102&&1&&20171010
        content = 'NULL&&NULL&&NULL&&{}&&'.format(each_data["sid"])
        return workload_key, content, workload_source

    @staticmethod
    def hilton(each_data):
        workload_source = each_data["source"] + "Hotel"
        workload_key = 'NULL|{}|{}'.format(each_data["sid"], workload_source)
        content = 'NULL&{}&{}&'.format(each_data["mid"], each_data["sid"])
        return workload_key, content, workload_source

    @staticmethod
    @retry(times=4)
    def create_table():
        create_sql = '''CREATE TABLE IF NOT EXISTS `workload_hotel_validation_new` (
      `id`           INT(11)    NOT NULL AUTO_INCREMENT,
      `workload_key` VARCHAR(256)        DEFAULT NULL,
      `content`      VARCHAR(256)        DEFAULT NULL,
      `source`       VARCHAR(64)         DEFAULT NULL,
      `extra`        TINYINT(1) NOT NULL,
      `status`       TINYINT(4) NOT NULL DEFAULT '1'
      COMMENT '0:close,1:open',
      `updatetime`   TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      UNIQUE KEY `_ix_workload_key` (`workload_key`),
      KEY `_ix_source` (`source`)
    )
      ENGINE = InnoDB
      DEFAULT CHARSET = utf8;'''
        truncate_sql = '''TRUNCATE workload_hotel_validation_new;'''
        conn = verify_info_pool.connection()
        cursor = conn.cursor()
        cursor.execute(create_sql)
        cursor.execute(truncate_sql)
        cursor.close()
        conn.close()

    @staticmethod
    @retry(times=4)
    def change_table():
        _change_sql_old = '''ALTER TABLE workload_hotel_validation
      RENAME workload_hotel_validation_old;'''
        _change_sql_new = '''ALTER TABLE workload_hotel_validation_new
      RENAME workload_hotel_validation;'''
        _delete_sql = '''DROP TABLE workload_hotel_validation_old;'''

        conn = verify_info_pool.connection()
        cursor = conn.cursor()
        cursor.execute(_change_sql_old)
        cursor.execute(_change_sql_new)
        cursor.execute(_delete_sql)
        cursor.close()
        conn.close()

    @func_time_logger
    @retry(times=4)
    def insert_data(self, data):
        replace_sql = '''REPLACE INTO workload_hotel_validation_new (workload_key, content, source, extra, status) 
        VALUES (%s, %s, %s, 0, 1);'''

        conn = verify_info_pool.connection()
        cursor = conn.cursor()
        _replace_count = cursor.executemany(replace_sql, data)
        conn.commit()
        cursor.close()
        conn.close()
        self.offset = self.pre_offset
        logger.debug(
            "[insert data][now count: {}][insert data: {}][replace_count: {}]".format(self.offset, len(data),
                                                                                      _replace_count))

    @retry(times=10000)
    def update_per_hotel_validation(self, env='test'):
        data = []

        if env == 'test':
            db_conf = test_db
        elif env == 'online':
            db_conf = online_db
        else:
            raise TypeError("Unknown Env: {}".format(env))

        sql = '''SELECT
          source,
          sid,
          uid,
          mid,
          name,
          name_en,
          hotel_url
        FROM hotel_unid LIMIT {},999999999999;'''.format(self.offset)
        for line in MysqlSource(db_conf, table_or_query=sql, size=10000, is_table=False, is_dict_cursor=True):
            source = line['source']
            try:
                if source in (
                        "ctripcn", "yundijie", "daolvApi", "dotwApi", "expediaApi", "gtaApi", "hotelbedsApi",
                        "innstantApi",
                        "jacApi", "mikiApi", "touricoApi"):
                    each_data = self.default_api_task_key_and_content(line)
                    data.append(each_data)
                elif source in ("expedia", "cheaptickets", "orbitz", "ebookers", "travelocity"):
                    # ep 系，使用 url 类型的
                    each_data = self.expedia(line, source=source)
                    data.append(each_data)
                elif source in ("hrs", "ctrip"):
                    # 单纯 sid 的
                    each_data = self.sid_only_key_and_content(line)
                    data.append(each_data)
                elif source in ("marriott",):
                    # 单纯 sid 的，两个 &&
                    each_data = self.sid_only_key_and_content(line, double_key=True)
                    data.append(each_data)
                elif source == "hilton":
                    # hilton 专用，content 前面多了一个 NULL
                    each_data = self.hilton(line)
                    data.append(each_data)
                elif source == "booking":
                    # booking 专用
                    each_data = self.booking(line)
                    data.append(each_data)
                elif source == "elong":
                    # elong 专用
                    each_data = self.elong(line)
                    data.append(each_data)
                elif source == "hotels":
                    # hotels 专用
                    each_data = self.hotels(line)
                    data.append(each_data)
                elif source == "agoda":
                    # agoda 专用
                    each_data = self.agoda(line)
                    data.append(each_data)
                elif source in (
                        "accor", "hoteltravelEN", "hoteltravel", "venere", "venereEN", "agodaApi",
                        "amoma",
                        "haoqiaoApi", "hostelworld", "hotelclub", "ihg", "kempinski", "starwoodhotels", "tongchengApi"):
                    # 不更新 workload validation
                    # hoteltravel, venere 源被下掉了
                    # 由于 ctrip 爬虫当前倒了，本次不更新 ctrip
                    '''
                    这些源当前不进行验证，不生成相关任务
                    agodaApiHotel
                    amomaHotel
                    haoqiaoApiHotel
                    hostelworldHotel
                    hotelclubHotel
                    ihgHotel
                    kempinskiHotel
                    starwoodhotelsHotel
                    '''
                    pass
                else:
                    logger.warning("[Unknown Source: {}]".format(source))
            except Exception as exc:
                logger.exception(msg="[make workload key has exception][source: {}]".format(source), exc_info=exc)
                raise exc

            self.pre_offset += 1
            if len(data) == 2000:
                # replace into validation data
                self.insert_data(data)
                data = []

        # replace into validation data
        self.insert_data(data)

    def start(self):
        self.create_table()
        self.update_per_hotel_validation(env='test')

        if self.table_can_be_used:
            self.change_table()


if __name__ == '__main__':
    update_hotel_validation = UpdateHotelValidation()
    update_hotel_validation.start()
