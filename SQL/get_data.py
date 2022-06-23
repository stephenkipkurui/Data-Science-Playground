import pandas as pd
import os
import env


def get_connection():

    user = env.user
    passw = env.password
    host = env.host
    db = str(input('Enter database name'))

    url = f'mysql+pymysql://{user}:{passw}@{host}/{db}'

    return url


def acquire_data(use_cache=True):

    farmers_market = 'farmers.csv'

    if os.path.exists(farmers_market) and use_cache:

        print('Reading from CSV..')

        df = pd.read_csv(farmers_market)

        return df

    qry = 'SELECT * FROM market_date_info'

    print('Rading SQL..')

    df = pd.read_sql(qry, get_connection())

    print('Saving csv to file locally..')

    df.to_csv(farmers_market, index=False)
