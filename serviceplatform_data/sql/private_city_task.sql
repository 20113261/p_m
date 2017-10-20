CREATE TABLE `private_city_task` (
  `id`              VARCHAR(16),
  `name`            VARCHAR(128),
  `name_en`         VARCHAR(100),
  `map_info`        VARCHAR(64),
  `source`          VARCHAR(32),
  `sid`             VARCHAR(128),
  `has_nearby_city` BOOL,
  PRIMARY KEY (`id`, `source`, `sid`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;