#!/usr/bin/env bash

DATE=`date +%Y_%m_%d_%H_%M_%S`
FILE_NAME="attr_lost_img_"$DATE

echo "Start: "$DATE
#./bin/ks3util ls -c mioji.conf -b mioji-attr -k ks_attr_file
/usr/java/latest/bin/java -classpath /search/hourong/ks3util-1.1.1/conf:/search/hourong/ks3util-1.1.1/lib/commons-lang-2.6.jar:/search/hourong/ks3util-1.1.1/lib/commons-lang3-3.3.2.jar:/search/hourong/ks3util-1.1.1/lib/commons-collections-3.1.jar:/search/hourong/ks3util-1.1.1/lib/commons-pool-1.5.5.jar:/search/hourong/ks3util-1.1.1/lib/ks3-kss-java-sdk-0.6.2.jar:/search/hourong/ks3util-1.1.1/lib/joda-time-2.9.9.jar:/search/hourong/ks3util-1.1.1/lib/httpclient-4.3.4.jar:/search/hourong/ks3util-1.1.1/lib/httpcore-4.3.2.jar:/search/hourong/ks3util-1.1.1/lib/commons-codec-1.6.jar:/search/hourong/ks3util-1.1.1/lib/commons-logging-1.1.3.jar:/search/hourong/ks3util-1.1.1/lib/jackson-databind-2.3.0.jar:/search/hourong/ks3util-1.1.1/lib/jackson-annotations-2.3.0.jar:/search/hourong/ks3util-1.1.1/lib/jackson-core-2.3.0.jar:/search/hourong/ks3util-1.1.1/lib/commons-io-2.5.jar:/search/hourong/ks3util-1.1.1/lib/logback-classic-1.1.8.jar:/search/hourong/ks3util-1.1.1/lib/logback-core-1.1.8.jar:/search/hourong/ks3util-1.1.1/lib/slf4j-api-1.7.21.jar:/search/hourong/ks3util-1.1.1/lib/jcl-over-slf4j-1.7.21.jar:/search/hourong/ks3util-1.1.1/lib/javax.inject-1.jar:/search/hourong/ks3util-1.1.1/lib/airline-2.3.0-20170405.094046-4.jar:/search/hourong/ks3util-1.1.1/lib/airline-io-2.3.0-20170405.094029-5.jar:/search/hourong/ks3util-1.1.1/lib/commons-collections4-4.0.jar:/search/hourong/ks3util-1.1.1/lib/ks3util-1.1.1.jar -Dapp.name=ks3util -Dapp.pid=15980 -Dapp.repo=/search/hourong/ks3util-1.1.1/lib -Dapp.home=/search/hourong/ks3util-1.1.1 -Dbasedir=/search/hourong/ks3util-1.1.1 com.kingsoft.ks3util.Ks3Util ls -c mioji.conf -b mioji-attr -k ks_attr_file
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

