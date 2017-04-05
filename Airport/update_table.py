import pymysql
import dataset

if __name__ == '__main__':
    import pandas

    # conn = pymysql.connect(user='root', password='1234', charset='utf8', db='AirPort')
    db = dataset.connect('mysql://root:1234@localhost/AirPort?charset=utf8')
    table = db['airport_new']
    res = pandas.read_csv('/Users/hourong/Downloads/AirPort 机器匹配 wiki 与我方城市-新表.csv').fillna('')
    for i in range(len(res)):
        line = res.irow(i)
        if line['IATA'] == '' or line['ID'] == '':
            continue
        data = {
            'iata_code': line['IATA'],
            'name_en': line['AirPort name'],
            'city_id': str(int(line['ID'])),
            'country': line['country'],
            'city': line['name_en'],
            'map_info': line['map_info']
        }
        table.upsert(row=data, keys=['iata_code'])
