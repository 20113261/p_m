##整合上线新城市基础数据部分
    1.添加信息城市：
        注意：保证city表是最新的
        首先配置city/config.py中的city_path指向新添城市文件所在路径，这里只支持execl文件格式
    2.airport表的修改以及共享机场：
        1)在修改之前要保证
        airport表是最新的。
        2)配置airport_path指向更新机场文件所在的路径,需要注意文件的模板格式，这里只支持csv文件格式
    3.城市图片的更新
        配置picture_path指向更新图片文件所在的路径
    只要是对数据表操作的都存入了10.10.223.258 base_data库。
    配置完之后运行 city/city_BaseData.py文件中的start_task函数
        