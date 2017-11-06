CREATE TABLE `white_list` (
  `id`    INT(11)     NOT NULL AUTO_INCREMENT,
  `md5`   VARCHAR(64) NOT NULL,
  `info`  JSON,
  `utime` TIMESTAMP(6)         DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY (`md5`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;