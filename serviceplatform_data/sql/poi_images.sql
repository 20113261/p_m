CREATE TABLE `poi_images_new` (
  `id`          BIGINT(20)    NOT NULL AUTO_INCREMENT,
  `file_name`   VARCHAR(100)  NOT NULL DEFAULT 'NULL',
  `source`      VARCHAR(30)   NOT NULL DEFAULT 'NULL',
  `sid`         VARCHAR(100)  NOT NULL DEFAULT 'NULL',
  `url`         TEXT,
  `pic_size`    VARCHAR(60)   NOT NULL DEFAULT 'NULL',
  `bucket_name` VARCHAR(128)  NOT NULL DEFAULT '',
  `url_md5`     VARCHAR(1024) NOT NULL DEFAULT 'NULL',
  `pic_md5`     VARCHAR(64)   NOT NULL DEFAULT '',
  `use`         VARCHAR(10)   NOT NULL DEFAULT 'NULL',
  `part`        VARCHAR(32)   NOT NULL DEFAULT 'NULL',
  `date`        TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `info`        JSON                   DEFAULT NULL,
  UNIQUE KEY `s_sid` (`source`, `sid`),
  KEY `id` (`id`),
  KEY `file_name` (`file_name`),
  KEY `date` (`date`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 91224071
  DEFAULT CHARSET = utf8
  PARTITION BY KEY (`source`, `sid`)
  PARTITIONS 4;