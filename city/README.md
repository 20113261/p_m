###整合上线新城市基础数据部分
####项目在city文件夹下

    * add_city.py:
        执行read_file函数,参数为新增城市文件路径及数据库配置。该函数的目的是将新增城市入库，并将新增城市ID写入到city_id.csv
        文件中
    *city_map_cityName.py:
        执行revise_pictureName函数,参数为图片文件路径。该函数的目的是修改图片的名称。
       
    *config.py
        存放配置
    
    *db_insert.py:
        执行shareAirport_insert函数。该函数的目的是读取share_airport.csv文件，将新生成的共享机场入库。
       
    *share_airport.py:
        执行update_share_airport函数。该函数的目的是为新增的城市添加共享机场，并将新增的共享机场写入share_airport.csv以及
        没有被共享机场的城市列表 city_list.csv
    
    *field_check.py:
        city_field_check函数，该函数目的是城市字段检查,将不合格的城市写入check_city.csv文件
        city_must_write_field函数,该函数的目的是城市必填字段检查,将不合格的城市写入check_empty_city.csv文件
        airport_must_write_field函数,该函数的目的是机场必填字段检查，将不合格的机场写入check_empty_airport.csv文件
        airport_field_check函数,该函数的目的是机场字段检查,将不合格的机场写入check_airport.csv文件
        check_repeat_airport函数,该函数的目的是检查新增机场在库中是否已经存在,将存在的机场写入check_repeat_airport.csv文件
        check_repeat_city函数,该函数的目的是检查新增城市在库中是否已经存在,将存在的城市写入check_repeat_city.csv
    
        