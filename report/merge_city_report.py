import pandas
from sqlalchemy import create_engine

if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://hourong:hourong@localhost:3306/data_prepare')
    for key, table in [('shop', 'shopping_tmp'), ('attr', 'attraction_tmp'), ('rest', 'restaurant_tmp')]:
        data = pandas.read_sql('select city_id, count(*) as total from {} group by city_id'.format(table), engine)
        data.to_csv('/tmp/{}_report.csv'.format(key))
