#!/usr/bin/env bash

DATE=`date +%Y_%m_%d_%H_%M_%S`
FILE_NAME="attr_lost_img_"$DATE

echo "Start: "$DATE
./bin/ks3util ls -c mioji.conf -b mioji-attr -k ks_attr_file
echo "End Download Ks Img: "$DATE
sleep 1
mysql -uhourong -phourong update_img -e "select distinct file_name from attr_bucket_relation" > all_attr
echo "End Output Ks Img: "$DATE
sleep 1
sort all_attr ks_attr_file ks_attr_file |uniq -u > /search/lost_img_output/$FILE_NAME
echo "Get not uploaded img finished: "$DATE

lost_count=`cat /search/lost_img_output/$FILE_NAME|wc -l`
let lost_count="lost_count-1"
echo "Lost Pic: "$lost_count

total=$(mysql -uhourong -phourong update_img -e "select count(distinct file_name) from attr_bucket_relation" --raw --batch -s)
python3 green_report.py attr $total $lost_count

post_query="title=景点图片缺失例行统计&mailto=hourong%40mioji.com%3Bluwanning%40mioji.com&content=景点图片缺失："$lost_count"%0A文件路径：""10.10.189.213::root/search/lost_img_output/"$FILE_NAME
echo $post_query
curl -d $post_query "http://10.10.150.16:9000/sendmail"

