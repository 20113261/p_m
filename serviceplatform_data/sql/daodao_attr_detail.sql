CREATE TABLE IF NOT EXISTS `%s` (
  `id` varchar(32) NOT NULL,
  `source` varchar(32) NOT NULL,
  `name` varchar(256) DEFAULT NULL,
  `name_en` varchar(256) DEFAULT NULL,
  `alias` varchar(256) DEFAULT NULL,
  `map_info` varchar(64) DEFAULT NULL,
  `city_id` varchar(64) NOT NULL,
  `source_city_id` varchar(16) DEFAULT NULL,
  `address` varchar(128) DEFAULT NULL,
  `star` float DEFAULT NULL,
  `recommend_lv` varchar(100) DEFAULT NULL,
  `pv` int(11) DEFAULT NULL,
  `plantocounts` int(11) DEFAULT NULL,
  `beentocounts` int(11) DEFAULT NULL,
  `overall_rank` int(11) DEFAULT NULL,
  `ranking` varchar(11) DEFAULT NULL,
  `grade` varchar(11) DEFAULT NULL,
  `grade_distrib` varchar(512) DEFAULT NULL,
  `commentcounts` int(11) DEFAULT NULL,
  `tips` text,
  `tagid` varchar(256) DEFAULT NULL,
  `related_pois` varchar(256) DEFAULT NULL,
  `nomissed` varchar(1024) DEFAULT NULL,
  `keyword` varchar(512) DEFAULT NULL,
  `cateid` varchar(128) DEFAULT NULL,
  `url` varchar(320) NOT NULL,
  `phone` varchar(64) DEFAULT NULL,
  `site` varchar(320) DEFAULT NULL,
  `imgurl` text,
  `commenturl` varchar(512) DEFAULT NULL,
  `introduction` text,
  `first_review_id` text,
  `opentime` varchar(1000) DEFAULT NULL,
  `price` varchar(1024) DEFAULT NULL,
  `recommended_time` varchar(1024) DEFAULT NULL,
  `wayto` varchar(1024) DEFAULT NULL,
  `prize` int(11) DEFAULT NULL,
  `traveler_choice` int(11) DEFAULT NULL,
  `utime` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`,`source`,`city_id`),
  KEY `_i_map_info` (`map_info`),
  KEY `utime` (`utime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;