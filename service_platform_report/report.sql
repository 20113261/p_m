DROP TABLE IF EXISTS serviceplatform_product_summary;
CREATE TABLE `serviceplatform_product_summary` (
  `id`         INT(11) NOT NULL AUTO_INCREMENT,
  `tag`        VARCHAR(64)      DEFAULT 'NULL',
  `source`     VARCHAR(64)      DEFAULT 'NULL',
  `crawl_type` VARCHAR(64)      DEFAULT 'NULL',
  `type`       VARCHAR(64)      DEFAULT 'NULL',
  `report_key` VARCHAR(64)      DEFAULT 'NULL',
  `num`        INT(11)          DEFAULT '0',
  `date`       CHAR(8)          DEFAULT NULL,
  `datetime`   CHAR(12)         DEFAULT 'NULL',
  `hour`       TEXT,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_type_date_hour` (`tag`, `source`, `type`, `crawl_type`, `report_key`, `date`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;


DROP TABLE IF EXISTS serviceplatform_product_error_summary;
CREATE TABLE `serviceplatform_product_error_summary` (
  `id`         INT(11) NOT NULL AUTO_INCREMENT,
  `tag`        VARCHAR(64)      DEFAULT 'NULL',
  `source`     VARCHAR(64)      DEFAULT 'NULL',
  `crawl_type` VARCHAR(64)      DEFAULT 'NULL',
  `type`       VARCHAR(64)      DEFAULT 'NULL',
  `error_code` VARCHAR(64)      DEFAULT '0',
  `num`        INT(11)          DEFAULT '0',
  `date`       CHAR(8)          DEFAULT NULL,
  `datetime`   CHAR(12)         DEFAULT 'NULL',
  `hour`       TEXT,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_type_date_hour` (`tag`, `source`, `type`, `crawl_type`, `error_code`, `date`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

# 任务进度统计
DROP VIEW IF EXISTS service_platform_product_report;
CREATE VIEW service_platform_product_report AS
  SELECT
    tag,
    source,
    crawl_type,
    CASE WHEN crawl_type = 'hotel'
      THEN round(sum(CASE WHEN type = 'List' AND report_key = 'Done'
        THEN num
                     ELSE 0 END) / 10, 2)
    ELSE sum(CASE WHEN type = 'List' AND report_key = 'Done'
      THEN num
             ELSE 0 END) END AS list_done,

    CASE WHEN crawl_type = 'hotel'
      THEN round(sum(CASE WHEN type = 'List' AND report_key = 'FinalFailed'
        THEN num
                     ELSE 0 END) / 10, 2)
    ELSE sum(CASE WHEN type = 'List' AND report_key = 'FinalFailed'
      THEN num
             ELSE 0 END) END AS list_final_failed,


    CASE WHEN crawl_type = 'hotel'
      THEN round(sum(CASE WHEN type = 'List' AND report_key = 'Failed'
        THEN num
                     ELSE 0 END) / 10, 2)
    ELSE sum(CASE WHEN type = 'List' AND report_key = 'Failed'
      THEN num
             ELSE 0 END) END AS list_failed,

    sum(CASE WHEN type = 'List' AND report_key = 'CityDone'
      THEN num
        ELSE 0 END)          AS list_city_done,

    CASE WHEN crawl_type = 'hotel'
      THEN round(sum(CASE WHEN type = 'List' AND report_key = 'All'
        THEN num
                     ELSE 0 END) / 10, 2)
    ELSE sum(CASE WHEN type = 'List' AND report_key = 'All'
      THEN num
             ELSE 0 END) END AS list_all,

    sum(CASE WHEN type = 'Detail' AND report_key = 'Done'
      THEN num
        ELSE 0 END)          AS detail_done,
    sum(CASE WHEN type = 'Detail' AND report_key = 'FinalFailed'
      THEN num
        ELSE 0 END)          AS detail_final_failed,
    sum(CASE WHEN type = 'Detail' AND report_key = 'Failed'
      THEN num
        ELSE 0 END)          AS detail_failed,
    sum(CASE WHEN type = 'Detail' AND report_key = 'All'
      THEN num
        ELSE 0 END)          AS detail_all,
    sum(CASE WHEN type = 'Images' AND report_key = 'Done'
      THEN num
        ELSE 0 END)          AS img_done,
    sum(CASE WHEN type = 'Images' AND report_key = 'FinalFailed'
      THEN num
        ELSE 0 END)          AS img_final_failed,
    sum(CASE WHEN type = 'Images' AND report_key = 'Failed'
      THEN num
        ELSE 0 END)          AS img_failed,
    sum(CASE WHEN type = 'Images' AND report_key = 'All'
      THEN num
        ELSE 0 END)          AS img_all,
    date
  FROM serviceplatform_product_summary
  GROUP BY tag, source, crawl_type, date
  ORDER BY tag, source, crawl_type;


SELECT *
FROM service_platform_product_report;

SELECT *
FROM serviceplatform_product_summary;

# 失败错误码统计
DROP VIEW IF EXISTS service_platform_product_error_code_report;
CREATE VIEW service_platform_product_error_code_report AS
  SELECT
    tag,
    source,
    crawl_type,
    type,
    sum(num)        AS total,
    sum(CASE WHEN error_code = 0
      THEN num
        ELSE 0 END) AS '0',
    sum(CASE WHEN error_code = 12
      THEN num
        ELSE 0 END) AS '12',
    sum(CASE WHEN error_code IN (21, 22, 23)
      THEN num
        ELSE 0 END) AS '21+22+23',
    sum(CASE WHEN error_code = 25
      THEN num
        ELSE 0 END) AS '25',
    sum(CASE WHEN error_code = 27
      THEN num
        ELSE 0 END) AS '27',
    sum(CASE WHEN error_code = 29
      THEN num
        ELSE 0 END) AS '29',
    sum(CASE WHEN error_code = 33
      THEN num
        ELSE 0 END) AS '33',
    sum(CASE WHEN error_code = 36
      THEN num
        ELSE 0 END) AS '36',
    sum(CASE WHEN error_code = 37
      THEN num
        ELSE 0 END) AS '37',
    sum(CASE WHEN error_code = 101
      THEN num
        ELSE 0 END) AS '101',
    sum(CASE WHEN error_code = 102
      THEN num
        ELSE 0 END) AS '102',
    sum(CASE WHEN error_code = 103
      THEN num
        ELSE 0 END) AS '103',
    sum(CASE WHEN error_code = 104
      THEN num
        ELSE 0 END) AS '104',
    sum(CASE WHEN error_code = 105
      THEN num
        ELSE 0 END) AS '105',
    sum(CASE WHEN error_code = 106
      THEN num
        ELSE 0 END) AS '106',
    sum(CASE WHEN error_code = 107
      THEN num
        ELSE 0 END) AS '107',
    sum(CASE WHEN error_code = 108
      THEN num
        ELSE 0 END) AS '108',
    date
  FROM serviceplatform_product_error_summary
  GROUP BY tag, source, crawl_type, type, date
  ORDER BY tag, source, crawl_type, type;

SELECT *
FROM service_platform_product_error_code_report;

SELECT *
FROM service_platform_product_error_code_report;

CREATE TABLE `serviceplatform_crawl_report_summary` (
  `id`         INT(11) NOT NULL AUTO_INCREMENT,
  `tag`        VARCHAR(32)      DEFAULT 'NULL',
  `source`     VARCHAR(32)      DEFAULT 'NULL',
  `type`       VARCHAR(16)      DEFAULT 'NULL',
  `error_type` VARCHAR(192)     DEFAULT '0',
  `num`        INT(11)          DEFAULT '0',
  `date`       CHAR(8)          DEFAULT NULL,
  `datetime`   CHAR(12)         DEFAULT 'NULL',
  `hour`       CHAR(4),
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_type_date_hour` (`tag`, `source`, `type`, `error_type`, `date`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

#     error_key = ['全量','数据源错误', '无 name、name_en', "中英文名字相反", "中文名中含有英文名", '坐标错误(NULL)',
#                  '坐标错误(坐标为空或坐标格式错误，除去NULL)', "经纬度重复", '坐标与所属城市距离过远', "距离过远坐标翻转后属于所属城市",
#                  '静态评分异常(评分高于10分)']

# 静态数据错误统计
DROP VIEW IF EXISTS service_platform_crawl_error_report;
CREATE VIEW service_platform_crawl_error_report AS
  SELECT
    tag,
    source,
    type,
    sum(CASE WHEN error_type = '全量'
      THEN num
        ELSE 0 END) AS 全量,
    sum(CASE WHEN error_type = '数据源错误'
      THEN num
        ELSE 0 END) AS 数据源错误,
    sum(CASE WHEN error_type = '无 name、name_en'
      THEN num
        ELSE 0 END) AS '无 name、name_en',
    sum(CASE WHEN error_type = '中英文名字相反'
      THEN num
        ELSE 0 END) AS 中英文名字相反,
    sum(CASE WHEN error_type = '中文名中含有英文名'
      THEN num
        ELSE 0 END) AS 中文名中含有英文名,
    sum(CASE WHEN error_type = '坐标错误(NULL)'
      THEN num
        ELSE 0 END) AS '坐标错误(NULL)',
    sum(CASE WHEN error_type = '坐标错误(坐标为空或坐标格式错误，除去NULL)'
      THEN num
        ELSE 0 END) AS '坐标错误(坐标为空或坐标格式错误，除去NULL)',
    sum(CASE WHEN error_type = '经纬度重复'
      THEN num
        ELSE 0 END) AS 经纬度重复,
    sum(CASE WHEN error_type = '坐标与所属城市距离过远'
      THEN num
        ELSE 0 END) AS 坐标与所属城市距离过远,
    sum(CASE WHEN error_type = '距离过远坐标翻转后属于所属城市'
      THEN num
        ELSE 0 END) AS 距离过远坐标翻转后属于所属城市,
    sum(CASE WHEN error_type = '静态评分异常(评分高于10分)'''
      THEN num
        ELSE 0 END) AS '静态评分异常(评分高于10分)',
    date
  FROM serviceplatform_crawl_report_summary
  GROUP BY tag, source, type, date;

SELECT *
FROM service_platform_crawl_error_report;

# Data Coverage Report
DROP TABLE IF EXISTS serviceplatform_data_coverage_summary;
CREATE TABLE IF NOT EXISTS `serviceplatform_data_coverage_summary` (
  `id`        INT(11) NOT NULL AUTO_INCREMENT,
  `tag`       VARCHAR(64)      DEFAULT 'NULL',
  `source`    VARCHAR(64)      DEFAULT 'NULL',
  `type`      VARCHAR(64)      DEFAULT 'NULL',
  `col_name`  VARCHAR(64)      DEFAULT '0',
  `col_count` INT(11)          DEFAULT '0',
  `date`      CHAR(8)          DEFAULT NULL,
  `datetime`  CHAR(12)         DEFAULT 'NULL',
  `hour`      TEXT,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_type_date_hour` (`tag`, `source`, `type`, `col_name`, `date`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

DROP VIEW IF EXISTS service_platform_hotel_data_coverage_report;
CREATE VIEW service_platform_hotel_data_coverage_report AS
  SELECT
    tag,
    source,
    sum(CASE WHEN col_name = 'total'
      THEN col_count
        ELSE 0 END) AS total,
    sum(CASE WHEN col_name = 'hotel_name'
      THEN col_count
        ELSE 0 END) AS hotel_name,
    sum(CASE WHEN col_name = 'hotel_name_en'
      THEN col_count
        ELSE 0 END) AS hotel_name_en,
    sum(CASE WHEN col_name = 'map_info'
      THEN col_count
        ELSE 0 END) AS map_info,
    sum(CASE WHEN col_name = 'star'
      THEN col_count
        ELSE 0 END) AS star,
    sum(CASE WHEN col_name = 'grade'
      THEN col_count
        ELSE 0 END) AS grade,
    sum(CASE WHEN col_name = 'review_num'
      THEN col_count
        ELSE 0 END) AS review_num,
    sum(CASE WHEN col_name = 'has_wifi'
      THEN col_count
        ELSE 0 END) AS has_wifi,
    sum(CASE WHEN col_name = 'is_wifi_free'
      THEN col_count
        ELSE 0 END) AS is_wifi_free,
    sum(CASE WHEN col_name = 'has_parking'
      THEN col_count
        ELSE 0 END) AS has_parking,
    sum(CASE WHEN col_name = 'is_parking_free'
      THEN col_count
        ELSE 0 END) AS is_parking_free,
    sum(CASE WHEN col_name = 'service'
      THEN col_count
        ELSE 0 END) AS service,
    sum(CASE WHEN col_name = 'img_items'
      THEN col_count
        ELSE 0 END) AS img_items,
    sum(CASE WHEN col_name = 'description'
      THEN col_count
        ELSE 0 END) AS description,
    sum(CASE WHEN col_name = 'accepted_cards'
      THEN col_count
        ELSE 0 END) AS accepted_cards,
    sum(CASE WHEN col_name = 'check_in_time'
      THEN col_count
        ELSE 0 END) AS check_in_time,
    sum(CASE WHEN col_name = 'check_out_time'
      THEN col_count
        ELSE 0 END) AS check_out_time,
    date
  FROM serviceplatform_data_coverage_summary
  WHERE type = 'hotel'
  GROUP BY tag, source, date;

# attr type
DROP VIEW IF EXISTS service_platform_attr_data_coverage_report;
CREATE VIEW service_platform_attr_data_coverage_report AS
  SELECT
    tag,
    source,
    sum(CASE WHEN col_name = 'total'
      THEN col_count
        ELSE 0 END) AS total,
    sum(CASE WHEN col_name = 'name'
      THEN col_count
        ELSE 0 END) AS name,
    sum(CASE WHEN col_name = 'name_en'
      THEN col_count
        ELSE 0 END) AS name_en,
    sum(CASE WHEN col_name = 'map_info'
      THEN col_count
        ELSE 0 END) AS map_info,
    sum(CASE WHEN col_name = 'address'
      THEN col_count
        ELSE 0 END) AS address,
    sum(CASE WHEN col_name = 'star'
      THEN col_count
        ELSE 0 END) AS star,
    sum(CASE WHEN col_name = 'grade'
      THEN col_count
        ELSE 0 END) AS grade,
    sum(CASE WHEN col_name = 'ranking'
      THEN col_count
        ELSE 0 END) AS ranking,
    sum(CASE WHEN col_name = 'commentcounts'
      THEN col_count
        ELSE 0 END) AS commentcounts,
    sum(CASE WHEN col_name = 'tagid'
      THEN col_count
        ELSE 0 END) AS tagid,
    sum(CASE WHEN col_name = 'imgurl'
      THEN col_count
        ELSE 0 END) AS imgurl,
    sum(CASE WHEN col_name = 'introduction'
      THEN col_count
        ELSE 0 END) AS introduction,
    sum(CASE WHEN col_name = 'phone'
      THEN col_count
        ELSE 0 END) AS phone,
    sum(CASE WHEN col_name = 'site'
      THEN col_count
        ELSE 0 END) AS site,
    sum(CASE WHEN col_name = 'opentime'
      THEN col_count
        ELSE 0 END) AS opentime,
    date
  FROM serviceplatform_data_coverage_summary
  WHERE type = 'attr'
  GROUP BY tag, source, date;

# shop type
DROP VIEW IF EXISTS service_platform_shop_data_coverage_report;
CREATE VIEW service_platform_shop_data_coverage_report AS
  SELECT
    tag,
    source,
    sum(CASE WHEN col_name = 'total'
      THEN col_count
        ELSE 0 END) AS total,
    sum(CASE WHEN col_name = 'name'
      THEN col_count
        ELSE 0 END) AS name,
    sum(CASE WHEN col_name = 'name_en'
      THEN col_count
        ELSE 0 END) AS name_en,
    sum(CASE WHEN col_name = 'map_info'
      THEN col_count
        ELSE 0 END) AS map_info,
    sum(CASE WHEN col_name = 'address'
      THEN col_count
        ELSE 0 END) AS address,
    sum(CASE WHEN col_name = 'star'
      THEN col_count
        ELSE 0 END) AS star,
    sum(CASE WHEN col_name = 'grade'
      THEN col_count
        ELSE 0 END) AS grade,
    sum(CASE WHEN col_name = 'ranking'
      THEN col_count
        ELSE 0 END) AS ranking,
    sum(CASE WHEN col_name = 'commentcounts'
      THEN col_count
        ELSE 0 END) AS commentcounts,
    sum(CASE WHEN col_name = 'tagid'
      THEN col_count
        ELSE 0 END) AS tagid,
    sum(CASE WHEN col_name = 'imgurl'
      THEN col_count
        ELSE 0 END) AS imgurl,
    sum(CASE WHEN col_name = 'introduction'
      THEN col_count
        ELSE 0 END) AS introduction,
    sum(CASE WHEN col_name = 'phone'
      THEN col_count
        ELSE 0 END) AS phone,
    sum(CASE WHEN col_name = 'site'
      THEN col_count
        ELSE 0 END) AS site,
    sum(CASE WHEN col_name = 'opentime'
      THEN col_count
        ELSE 0 END) AS opentime,
    date
  FROM serviceplatform_data_coverage_summary
  WHERE type = 'shop'
  GROUP BY tag, source, date;

# rest type
DROP VIEW IF EXISTS service_platform_rest_data_coverage_report;
CREATE VIEW service_platform_rest_data_coverage_report AS
  SELECT
    tag,
    source,
    sum(CASE WHEN col_name = 'total'
      THEN col_count
        ELSE 0 END) AS total,
    sum(CASE WHEN col_name = 'name'
      THEN col_count
        ELSE 0 END) AS name,
    sum(CASE WHEN col_name = 'name_en'
      THEN col_count
        ELSE 0 END) AS name_en,
    sum(CASE WHEN col_name = 'map_info'
      THEN col_count
        ELSE 0 END) AS map_info,
    sum(CASE WHEN col_name = 'address'
      THEN col_count
        ELSE 0 END) AS address,
    sum(CASE WHEN col_name = 'grade'
      THEN col_count
        ELSE 0 END) AS grade,
    sum(CASE WHEN col_name = 'ranking'
      THEN col_count
        ELSE 0 END) AS ranking,
    sum(CASE WHEN col_name = 'commentcounts'
      THEN col_count
        ELSE 0 END) AS commentcounts,
    sum(CASE WHEN col_name = 'cuisines'
      THEN col_count
        ELSE 0 END) AS cuisines,
    sum(CASE WHEN col_name = 'imgurl'
      THEN col_count
        ELSE 0 END) AS imgurl,
    sum(CASE WHEN col_name = 'introduction'
      THEN col_count
        ELSE 0 END) AS introduction,
    sum(CASE WHEN col_name = 'phone'
      THEN col_count
        ELSE 0 END) AS phone,
    sum(CASE WHEN col_name = 'site'
      THEN col_count
        ELSE 0 END) AS site,
    sum(CASE WHEN col_name = 'opentime'
      THEN col_count
        ELSE 0 END) AS opentime,
    date
  FROM serviceplatform_data_coverage_summary
  WHERE type = 'rest'
  GROUP BY tag, source, date;

# qyer total type
DROP VIEW IF EXISTS service_platform_total_data_coverage_report;
CREATE VIEW service_platform_total_data_coverage_report AS
  SELECT
    tag,
    source,
    sum(CASE WHEN col_name = 'total'
      THEN col_count
        ELSE 0 END) AS total,
    sum(CASE WHEN col_name = 'name'
      THEN col_count
        ELSE 0 END) AS name,
    sum(CASE WHEN col_name = 'name_en'
      THEN col_count
        ELSE 0 END) AS name_en,
    sum(CASE WHEN col_name = 'map_info'
      THEN col_count
        ELSE 0 END) AS map_info,
    sum(CASE WHEN col_name = 'address'
      THEN col_count
        ELSE 0 END) AS address,
    sum(CASE WHEN col_name = 'star'
      THEN col_count
        ELSE 0 END) AS star,
    sum(CASE WHEN col_name = 'grade'
      THEN col_count
        ELSE 0 END) AS grade,
    sum(CASE WHEN col_name = 'ranking'
      THEN col_count
        ELSE 0 END) AS ranking,
    sum(CASE WHEN col_name = 'commentcounts'
      THEN col_count
        ELSE 0 END) AS commentcounts,
    sum(CASE WHEN col_name = 'tagid'
      THEN col_count
        ELSE 0 END) AS tagid,
    sum(CASE WHEN col_name = 'imgurl'
      THEN col_count
        ELSE 0 END) AS imgurl,
    sum(CASE WHEN col_name = 'introduction'
      THEN col_count
        ELSE 0 END) AS introduction,
    sum(CASE WHEN col_name = 'phone'
      THEN col_count
        ELSE 0 END) AS phone,
    sum(CASE WHEN col_name = 'site'
      THEN col_count
        ELSE 0 END) AS site,
    sum(CASE WHEN col_name = 'opentime'
      THEN col_count
        ELSE 0 END) AS opentime,
    date
  FROM serviceplatform_data_coverage_summary
  WHERE type = 'total'
  GROUP BY tag, source, date;

SELECT *
FROM service_platform_hotel_data_coverage_report;

SELECT *
FROM service_platform_attr_data_coverage_report;

SELECT *
FROM service_platform_total_data_coverage_report;

SELECT *
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'Report' AND TABLE_NAME = 'service_platform_attr_data_coverage_report'

SELECT *
FROM service_platform_product_report
WHERE date = '20170926';
