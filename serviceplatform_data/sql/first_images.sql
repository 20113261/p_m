CREATE TABLE `first_images` (
  `id`          BIGINT(20)   NOT NULL AUTO_INCREMENT,
  `source`      VARCHAR(64)  NOT NULL,
  `source_id`   VARCHAR(128) NOT NULL,
  `first_img`   VARCHAR(96)           DEFAULT NULL,
  `utime`       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `_unique_ix_source` (`source`, `source_id`),
  KEY `_ix_utime` (`utime`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;