CREATE TABLE `white_list` (
  `id`    INT(11)     NOT NULL AUTO_INCREMENT,
  `type`  VARCHAR(64) NOT NULL,
  `md5`   VARCHAR(64) NOT NULL,
  `info`  JSON,
  `utime` TIMESTAMP(6)         DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `_unique_ix_md5` (`md5`),
  KEY `_ix_type` (`type`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;