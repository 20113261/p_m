#!/usr/bin/env bash

DATE=`date +%Y_%m_%d_%H_%M_%S`
FILE_NAME="rest_lost_img_"$DATE

echo "Start: "$DATE
./bin/ks3util ls -c mioji.conf -b mioji-rest -k ks_rest_file
echo "End Download Ks Img: "$DATE
sleep 1
mysql -uhourong -phourong update_img -e "select distinct file_name from rest_bucket_relation" > all_rest
echo "End Output Ks Img: "$DATE
sleep 1
sort all_rest ks_rest_file ks_rest_file |uniq -u > /search/lost_img_output/$FILE_NAME
echo "Get not uploaded img finished: "$DATE

lost_count=`cat /search/lost_img_output/$FILE_NAME|wc -l`
let lost_count="lost_count-1"
echo "Lost Pic: "$lost_count

total=$(mysql -uhourong -phourong update_img -e "select count(distinct file_name) from rest_bucket_relation" --raw --batch -s)
python3 green_report.py rest $total $lost_count

post_query="title=餐厅图片缺失例行统计&mailto=hourong%40mioji.com%3Bluwanning%40mioji.com&content=餐厅图片缺失："$lost_count"%0A文件路径：""10.10.189.213::root/search/lost_img_output/"$FILE_NAME
echo $post_query
curl -d $post_query "http://10.10.150.16:9000/sendmail"

