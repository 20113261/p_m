CREATE TABLE IF NOT EXISTS `%s` (
  `id` varchar(32) NOT NULL,
  `source` varchar(32) NOT NULL,
  `name` varchar(256) DEFAULT NULL,
  `name_en` varchar(256) DEFAULT NULL,
  `name_py` varchar(128) DEFAULT NULL,
  `alias` varchar(256) DEFAULT NULL,
  `map_info` varchar(64) DEFAULT NULL,
  `city_id` varchar(64) DEFAULT NULL,
  `source_city_id` varchar(16) NOT NULL,
  `source_city_name` varchar(256) DEFAULT NULL,
  `source_city_name_en` varchar(256) DEFAULT NULL,
  `source_country_id` varchar(16) DEFAULT NULL,
  `source_country_name` varchar(256) DEFAULT NULL,
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
  `url` varchar(512) DEFAULT NULL,
  `phone` varchar(64) DEFAULT NULL,
  `site` varchar(256) DEFAULT NULL,
  `imgurl` varchar(2048) DEFAULT NULL,
  `commenturl` varchar(512) DEFAULT NULL,
  `introduction` text,
  `opentime` varchar(1000) DEFAULT NULL,
  `price` varchar(1024) DEFAULT NULL,
  `recommended_time` varchar(1024) DEFAULT NULL,
  `wayto` varchar(1024) DEFAULT NULL,
  `crawl_times` int(11) DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  `insert_time` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `flag` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`source`,`source_city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;