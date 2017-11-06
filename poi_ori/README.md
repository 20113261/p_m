# POI 融合顺序

## 数据准备

- 确保 test 环境中 base_data 以及 data_process 均为最新版本 (或者指定的旧版本，需要保持两者版本一致)
- 备份关联表 attr_unid 并清空
- 清空 unknown_keywords
- 确保 already_merged_city 中没有 poi_type 以及 poi_type_data 例如：attr 、attr_data 两种类型
- 清空 filter_data_already_online
- 当完成以上操作后，调用 prepare_data.py 更新融合需要相关数据

## 数据融合

### 对应关系融合

- python3 multi_city.py attr/shop (景点、购物融合方法)

### 数据融合

本部分对 cpu 以及内存要求比较高，所以可以考虑在 cpmaster02 机器上面运行

- 修改其中的 poi_type 为 attr、shop
- python3 init_multi_city_insert_db.py

### 购物融合后的 outlets 融合

- python3 outlets_merge.py 融合 outlets 相关数据

### 数据融合后的图片融合

img_ori/poi_img.py

- 调整需要的 poi_type
- python3 poi_img.py 

## 数据生成并提交表格

本脚本操作需要注意，会同时更新 test 环境以及提交 base_data 上线数据，需要确保之前的所有操作正确后，数据正确后，再进行最后的提交，其中需要判定表名称以及 poi_type 而进行相应的操作

- python3 mk_base_data_final.py 