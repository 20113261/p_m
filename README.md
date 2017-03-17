# POI 上线脚本

## 景点上线脚本
### 上线规则：
1. 根据数据来源确定必要的不为空且内容不为 0 的字段：
 - 来源中包括到到的：name , name_en , map_info ，norm_tagid
 - 来源为到到之外其他源的：name , name_en , map_info , norm_tagid , first_image
2. 检查 open 字段。如为空，则统一填为：<*><*><00:00-23:55><SURE>
3. 检查 address 字段。可为空，但内容不能为 0

### 脚本使用
``` bash
cd new_attraction_handling
python3 updatPoi.py cid_file 
```
**cid_file 结构**
- city_id, name, name_en, country
- 四个字段中间用 \t 隔开

### 结束后会生成报表
如下表：

|ID|城市名|国家|开发库中景点个数|test上线景点个数|上线景点占比|上线景点有图占比|开发库中daodao景点个数|上线daodao景点个数|上线daodao景点占比|
|----|----|----|----|----|----|----|----|----|----|
|vxxxxxx | | | | | | | | | | |


## 购物上线脚本
### 上线规则：
1. 以下字段均不为空且内容不为 0（其中 name_en 如为空可用 name 填充）：
name、name_en、map_info、city、first_image、norm_tagid

2. 检查 open 字段。如为空，则统一填为：<*><*><08:00-20:00><SURE>

### 脚本使用
``` bash
cd new_shopping_handling
python3 updatPoi.py cid_file 
```
**cid_file 结构**
- city_id, name, name_en, country
- 四个字段中间用 \t 隔开

### 结束后会生成报表
如下表：

|ID|城市名|国家|开发库中景点个数|test上线景点个数|上线景点占比|上线景点有图占比|开发库中daodao景点个数|上线daodao景点个数|上线daodao景点占比|
|----|----|----|----|----|----|----|----|----|----|
|shxxxxxx | | | | | | | | | | |

## 餐厅上线脚本
### 上线规则：
1. 以下 5 个字段均不为空且内容不为 0（其中 name_en 如为空可用 name 填充）：
name、name_en、map_info、city、first_image

2. 检查 open_time 字段。如为空，则统一填为：<*><*><00:00-23:55><SURE>

### 脚本使用
``` bash
cd new_restaurant_handling
python3 updatPoi.py cid_file 
```
**cid_file 结构**
- city_id, name, name_en
- 四个字段中间用 \t 隔开

# POI 融合流程

1. 生成本次融合的 city_id find_city_id/* 下,通过 get_city_id.py 返回本次融合的所有 city_id

2. 分别在 *_merge 中处理数据,生成融合后的数据,注意在使用 *_merge.py 中需要指定融合的多种源,通过修改 TASK_SOURCE 的值来指定

  - 特殊,奥特莱斯字段:
    - shop_merge/*
      - outlets_merge.py 获取 outlets.csv
      - get_update_sql.py 从 outlets.csv 中获取 update 语句

3. 补充,修改字段

 - add_open_time/add_open_time.py open_time 字段

 - norm_tag/*  更新 norm_tag 字段
   - attr_norm_tag.py
   - rest_norm_tag.py
   - shop_norm_tag.py

 - price_level_change/price_level.py 规范化 price_level 字段


4, 图片,评论字段

图片
 - common_script/export_img_url.py 导出图片链接 (已废弃)
 - error_image/export_img_url.py 导出图片链接 (已废弃)
 - 当前图片抓取方式：在 serviceplatform 平台中启动 daodao_img 相关任务，抓取 daodao 图片链接
 - 配置图片链接文件名文件名后,启动 celery 中的 get_img 任务,图片会抓在 nfs 的 10.10.213.148:/data/image_celery/任务名_celery 中,挂载在本地的 /data/image/任务名_celery

 - 启动 celery 中的 get_images_info 任务,更新图片的基本信息,存在表 image_info 中

- poi_img/*
  - export_img_url.py 导出图片链接并将抓取得到的 image 添加到相应的字段上，data_prepare 中的字段需要进行修改，修改城市范围以及 poi 类型
  - add_dict.py 在 Redis 中创建用于图片更新的键值对
  - copy_file_for_first_time.py 用于第一次补充图片, 此时图片原始链接在 image 上 (与 copy_file 二选一)
  - copy_file.py 图片改名并放置于正确文件夹 (与 copy_file_for_first_time 二选一)
  - update_table/update_dev_img.py 更新 image_list 字段
  - update_table/update_dev_img_by_failed_img.py 通过比率小于 0.9 的被过滤的图片更新无图 POI


5. 评论

 - 启动 celery 中 get_comment 任务,抓取短评论和长评论链接,存储于 comment_1101 和 long_1101 中
 - 启动 celery 中 get_long_comment 任务,从 long_1101 中读取链接,抓取评论并存储于 comment_1101 中

 - poi_comment/*
   - update_comment_by_list_new.py 需要设置 city_id_set，target_db，target_table 字段，并根据此从 Comment.tp_comment 中读取评论信息更新目标表


5. first_img/update_first_img.py 更新 first_img 字段

6. 清洗
最佳运行顺序如下
 - data_clean/*
   - attraction_clean_by_tag 清除游览 poi
   - attraction_clean_by_tag_2 清除非景点 poi
   - shopping_clean_by_tag 清除非购物 poi
   - attraction_clean 清除名字中的错误
   - attr_add_name 给无 name 的 poi 添加 name
   - delete_empty_map_info_poi 删除 POI map_info 为空的点

7. 恢复 introduction、intensity、 rcmd_intensity 字段
 - keywords_backup/*
   - keywords_backup.py 恢复以上 3 字段信息

 - 导出距离城市中心大于 40 km 的 POI 点
 - error_distance/*
   - get_error_distance_poi 运行时需要调整其中的 TYPE , POI_TABLE ,DISTANCE 的值。
   - 运行后会导出到:
     - /tmp/error_distance_poi_TYPE.csv 存放错误的 POI 点的 id 以及相关信息
     - /tmp/error_distance_city_TYPE.csv 存放错误的城市 city_id 以及城市中错误 POI 点的概率
     - /tmp/error_distance_reverse_TYPE.csv 存放经纬度翻转的 POI 点的相关信息

 - 融合城市报告
 - report/*
   - merge_city_report 导出融合城市以及 poi 的对应关系

7. 导出交付

# 抓取数据字段情况统计

## 运行方法
- 修改 info_count/info_count.py 中的数据库，数据表信息，以及需要导出报告结果的文件类型和文件位置
- `python3 info_count.py`


# 通用数据更新脚本

## 修改相关配置

- SQL_DICT 中填写数据库连接的相关信息，host，user，password，charset，table 等相关信息
- table 为 pandas 的一个 DataFrame 其中配置文件名，sheet 名字，以及每个 sheet 中第几行为数据表表头
- table_name 数据表的名称
- search_keys 用于生成 where 语句中的约束条件，支持多个
- ignore_cols 忽略的列，在表格中会出现部分隐藏的列，对于这种隐藏的列程序是无法直接判断的，需要手动添加隐藏的是哪一列

## 运行脚本

```bash
python3 update_table.py
```

# 新增城市脚本

## 环境 

- python3
- pandas
- dataset

使用 python3.5 环境开发， pandas 作为文件输入部分，dataset 作为插入数据库部分

## 配置
```python
    # 配置为 city spiderdb 或 test 或 online 的数据库
    SQL_DICT = {
        'host': '10.10.154.38',
        'user': 'writer',
        'password': 'miaoji1109',
        'charset': 'utf8',
        'db': 'devdb'
    }
    # excel 文件位置
    xlsx_path = '/Users/hourong/Downloads/2_new_city.xlsx'
    # 是否反转经纬度
    need_change_map_info = False
    # 更新数据库地址
    target_db = 'mysql://hourong:hourong@10.10.180.145/data_prepare?charset=utf8'
    # 更新数据表地址
    target_table = 'city'
```
## 启动方法
```bash
python3 add_city.py
```