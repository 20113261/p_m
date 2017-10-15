#!/usr/bin/env bash

# 使用 country 进行测试，导出 replace sql 数据
mysqldump -h10.10.69.170 -ureader -pmiaoji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert base_data country| sed 's/`country`/`test_country`/g'

# 导出酒店旧表
mysqldump -h10.10.189.213 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert update_img pic_relation| sed 's/`pic_relation`/`hotel_images`/g' >  pic_relation.sql

# 导出酒店 0905_old
mysqldump -h10.10.189.213 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert update_img pic_relation_0905_old| sed 's/`pic_relation_0905_old`/`hotel_images`/g' > pic_relation_0905_old.sql

# 导出酒店 0914
mysqldump -h10.10.189.213 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert update_img pic_relation_0914| sed 's/`pic_relation_0914`/`hotel_images`/g' > pic_relation_0914.sql

# 0905
mysqldump -h10.10.189.213 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert update_img pic_relation_0905| sed 's/`pic_relation_0905`/`hotel_images`/g' > pic_relation_0905.sql

# 使用优先级 pic_relation < 0905_old < 0914 < 0905

# POI
# poi_bucket_relation
mysqldump -h10.10.189.213 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert update_img poi_bucket_relation| sed 's/`poi_bucket_relation`/`poi_images`/g' > poi_bucket_relation.sql

# poi_bucket_relation_0925
mysqldump -h10.10.189.213 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert update_img poi_bucket_relation_0925| sed 's/`poi_bucket_relation_0925`/`poi_images`/g' > poi_bucket_relation_0925.sql


# attr detail old
mysqldump -h10.10.180.145 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert attr_merge attr| sed 's/`insert_time`/`utime`/g' > attr.sql

# rest detail test
mysqldump -h10.10.180.145 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert --where="1 limit 10000" rest_merge rest| sed 's/`description`/`introduction`/g' |sed 's/`insert_time`/`utime`/g'|sed 's/`image_url`/`imgurl`/g'|sed 's/`review_num`/`commentcounts`/g'|sed 's/`real_ranking`/`ranking`/g' > rest.sql

# rest detail old

# shop detail old
mysqldump -h10.10.180.145 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert shop_merge shop |sed 's/`insert_time`/`utime`/g'> shop.sql