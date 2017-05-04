import pandas
import dataset


def update_region(file_path):
    try:
        file = pandas.read_csv(
            file_path
        )
        converters = {key: str for key in file.keys()}
        file = pandas.read_csv(
            file_path,
            converters=converters,
        ).fillna('null')

    except Exception as e:
        print("文件读取错误 " + str(e))
        return None

    target_db = dataset.connect('mysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**SQL_DICT))
    table = target_db['city']

    for i in range(len(file)):
        line = file.irow(i)
        print('#' * 100)
        print(dict(line))
        print('Now', i)
        if not Debug:
            print('Update', table.update(line, keys=['id']))


if __name__ == '__main__':
    SQL_DICT = {
        'host': '10.10.154.38',
        'user': 'writer',
        'password': 'miaoji1109',
        'charset': 'utf8',
        'db': 'devdb'
    }
    Debug = False
    file_path = '/Users/hourong/Downloads/city_region_0428.csv'
    update_region(file_path)
