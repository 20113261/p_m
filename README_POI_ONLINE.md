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


