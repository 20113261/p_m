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
mysqldump -h10.10.180.145 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert rest_merge rest| sed 's/`description`/`introduction`/g' |sed 's/`insert_time`/`utime`/g'|sed 's/`image_url`/`imgurl`/g'|sed 's/`review_num`/`commentcounts`/g'|sed 's/`real_ranking`/`ranking`/g' > rest.sql

# shop detail old
mysqldump -h10.10.180.145 -uhourong -phourong --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert shop_merge shop |sed 's/`insert_time`/`utime`/g'> shop.sql


# attr new data init
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert BaseDataFinal attr_final_20170928a > attr_final_20170928a.sql

mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert BaseDataFinal attr_final_20170929a |sed 's/`attr_final_20170929a`/`attr`/g' > attr_final_20170929a.sql

mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert BaseDataFinal attr_final_20171010a |sed 's/`attr_final_20171010a`/`attr`/g' > attr_final_20171010a.sql

mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert BaseDataFinal attr_final_20170929a |sed 's/`attr_final_20170929a`/`shop`/g' > shop_final_20170929a.sql

mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert BaseDataFinal attr_final_20171010a |sed 's/`attr_final_20171010a`/`shop`/g' > shop_final_20171010a.sql

# elong 更新数据导出
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --set-gtid-purged=OFF --no-create-info --no-create-db --complete-insert --where="source='elong'" BaseDataFinal hotel_final_20170929a |sed 's/`hotel_final_20170929a`/`hotel_final_elong`/g' > hotel_final_elong_20170929a.sql

# elong 1010a 更新数据导出
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --set-gtid-purged=OFF --no-create-info --no-create-db --complete-insert --where="source='elong'" BaseDataFinal hotel_final_20171010a |sed 's/`hotel_final_20171010a`/`hotel_final_elong`/g' > hotel_final_elong_20171010a.sql

# elong data final
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --set-gtid-purged=OFF --no-create-info --no-create-db --complete-insert --where="source='elong'" BaseDataFinal hotel_final_elong  > hotel_final_elong.sql

# init hotel data into mongo (use city as test case)
mysqldump -h10.10.69.170 -ureader -pmiaoji1109 -fields-terminated-by=, base_data city > city.txt

mysqldump -h10.10.69.170 -ureader -pmiaoji1109 --fields-terminated-by=, --tab=[DIR TO SAVE TO] --tables base_data city > city.txt

mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 -T /tmp base_data city

mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --fields-terminated-by="," --fields-enclosed-by="" --fields-escaped-by="" --no-create-db --no-create-info --tab="." information_schema CHARACTER_SETS base_data city

mysql -h10.10.228.253 -umioji_admin -pmioji1109 --database=base_data --execute='SELECT `FIELD`, `FIELD` FROM `TABLE` LIMIT 0, 10000 ' -X > file.csv

# insert img data
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --skip-lock-tables --insert-ignore --no-create-info --no-create-db --complete-insert --where "id > 6712287 order by id" ServicePlatform images_attr_daodao_20170929a |sed 's/`images_attr_daodao_20170929a`/`poi_images`/g' > images_attr_daodao_20170929a_new.sql

# insert PoiPicInformation
mysqldump -h10.10.154.38 -ureader -pmiaoji1109 --skip-lock-tables --insert-ignore --no-create-info --no-create-db --complete-insert devdb PoiPictureInfomation | sed 's/`PoiPictureInfomation`/`PoiPictureInformation`/g' > PoiPictureInformation.sql

# init hotel images
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --skip-lock-tables --insert-ignore --no-create-info --no-create-db --complete-insert  BaseDataFinal hotel_images | sed 's/`hotel_images`/`hotel_images_new`/g' > hotel_images.sql

# init poi image
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --skip-lock-tables --insert-ignore --no-create-info --no-create-db --complete-insert  BaseDataFinal poi_images | sed 's/`poi_images`/`poi_images_new`/g' > poi_images.sql

# init validation task
mysqldump -h10.10.230.206 -umioji_admin -pmioji1109 --skip-lock-tables --insert-ignore --no-create-info --no-create-db --complete-insert  verify_info workload_hotel_validation_new | sed 's/`workload_hotel_validation_new`/`workload_hotel_validation`/g' > workload_hotel_validation.sql

# 酒店数据导出
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert BaseDataFinal hotel_final_20171122a |sed 's/`hotel_final_20171122a`/`hotel_final`/g' > hotel_final_20171122a.sql

mysqldump -h10.10.238.148 -umioji_admin -pmioji1109 --skip-lock-tables --no-create-info --no-create-db tmp hotel_final > hotel_final_new.sql

mysqldump -h10.10.213.148 -umioji_admin -pmioji1109 --insert-ignore --skip-lock-tables --no-create-info --no-create-db --where="1 limit 3000000" tmp hotel_final |sed 's/`hotel_final`/`hotel_final_1`/g' > hotel_final_1.sql

mysqldump -h10.10.213.148 -umioji_admin -pmioji1109 --insert-ignore --skip-lock-tables --no-create-info --no-create-db --where="1 limit 6000000,99999999999" tmp hotel_final |sed 's/`hotel_final`/`hotel_final_2`/g' > hotel_final_2.sql

mysqldump -h10.10.213.148 -umioji_admin -pmioji1109 --insert-ignore --skip-lock-tables --no-create-info --no-create-db --where="1 limit 3000000,3000000" tmp hotel_final |sed 's/`hotel_final`/`hotel_final_3`/g' > hotel_final_3.sql

# qyer 图片数据合并
mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 --replace --skip-lock-tables --no-create-info --no-create-db --complete-insert --allow-keywords "" ServicePlatform  |sed 's/`images_total_qyer_20171120a`/`poi_images`/g' > images_total_qyer_20171120a.sql

mysqldump -h10.19.153.98 -umioji_admin -pmioji1109 --insert-ignore --skip-lock-tables --no-create-info --no-create-db ServicePlatform workload_train > workload_train_1226.sql

