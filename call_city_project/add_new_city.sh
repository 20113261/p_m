#!/bin/bash

echo "Hello world";

#将zip 文件解压到指定文件夹
rsync -a 10.10.150.16::opcity/ /search/service/nginx/html/MioaPyApi/store/opcity/
#echo "第一个参数：$1";
unzip -d /search/service/nginx/html/MioaPyApi/store/opcity/$3 $1

#导出数据表
mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data city > /search/cuixiyi/city.sql
mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data airport > /search/cuixiyi/airport.sql
#mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data chat_attraction > /search/cuixiyi/chat_attraction.sql
<<<<<<< HEAD
#mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data hotel > /search/cuixiyi/hotel.sql
=======
mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data hotel > /search/cuixiyi/hotel.sql
>>>>>>> c6191816c89d6f5b0ff6abca0911cfae84d85e83
#mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data station > /search/cuixiyi/station.sql
#mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data chat_shopping > /search/cuixiyi/chat_shopping.sql

mysql -h10.10.230.206 -umioji_admin -pmioji1109 -e "create database $2"

#将数据表导入临时库
mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 < /search/cuixiyi/city.sql
mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 < /search/cuixiyi/airport.sql
#mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 <  /search/cuixiyi/chat_attraction.sql
<<<<<<< HEAD
#mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 <  /search/cuixiyi/hotel.sql
#mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 </search/cuixiyi/station.sql
#mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 < /search/cuixiyi/chat_shopping.sql

#将生成的sql 文件删除
rm -rf /search/cuxiyi/city.sql /search/cuixiyi/airport.sql /search/cuixiyi/hotel.sql

=======
mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 <  /search/cuixiyi/hotel.sql
#mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 </search/cuixiyi/station.sql
#mysql -h10.10.230.206 -umioji_admin -pmioji1109 $2 < /search/cuixiyi/chat_shopping.sql

>>>>>>> c6191816c89d6f5b0ff6abca0911cfae84d85e83


