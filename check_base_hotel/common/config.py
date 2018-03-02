#encoding=utf-8
dev_db_ip = '10.10.69.170'
dev_db_name = 'base_data'
dev_db_user = 'reader'
dev_db_pwd = 'miaoji1109'

dev_db_ip_new = '10.10.242.173'
dev_db_name_new = 'data_process'
dev_db_user_new = 'root'
dev_db_pwd_new = 'shizuo0907'

#pika_host="10.10.155.37"
#pika_port=9221
#pika_pwd="MiojiPikaOrz"
pika_host="127.0.0.1"
pika_port=9229
pika_pwd="MiojiPikaOrz"

#原始数据存储机器配置
ori_db_ip = '10.10.155.37'
ori_db_user = 'root'
ori_db_pwd = ''
ori_db_name = 'ori_hotel_data_from_parser'
ori_table_name = "hotel_static_data_from_parse"

#融合数据存储机器
local_db_ip = '127.0.0.1'
local_db_user = 'root'
local_db_pwd = ''

#已融合的酒店数据
db_name = 'test'
base_hotel_table_name = 'hotel_before_merge'
base_unid_table = 'hotel_unid_before_merge'

#融合后的数据
unid_table_name = 'hotel_unid'
hotel_table_name = 'hotel'

#待融合数据
#unmerged_table_list = ['hotel_static_data_from_parse_uk']
#unmerged_table_list = ['hotel_static_data_from_parse']
#unmerged_table_list = ['hotelinfo_hotels_51494']
unmerged_table_list = ['hotel_final_20170102a','hotel_final_20171214a','hotel_final_20171226a']

#max_uid = 30889530

