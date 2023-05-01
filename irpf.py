import pandas as pd
import numpy as np
import datetime as dt
from time import time

from lib import tools, mysql_lib
from lib.constants import *

def reset_table(b):
    if 'drop' in db[b]:
        mysql_lib.execute_query(db[b]['drop'], mode='management')
        mysql_lib.execute_query(db[b]['create'], mode='management')


def calculate_avg_prices():
    print('Executing irpf base query...')
    df = mysql_lib.execute_query('queries/irpf/irpf_raw.sql')
    df['avg_price'] = 0
    avg_price_list = []
    sales = []
    print('Calculating avg price and sales...')
    for t in df['ticker'].unique():
        df_t = df.loc[df['ticker'] == t]
        avg_price = 0
        for index, row in df_t.iterrows():
            if round(row['total_units'], 6) <= 0:
                avg_price = avg_price
            elif row['units'] > 0:
                avg_price = (avg_price * (row['total_units'] - row['acquired_units']) + row['acquired_value']) / row['total_units']
            avg_price_list.append([row['id'] ,row['ticker'], row['calendar_date'], avg_price, row['total_units'], avg_price * max(row['total_units'], 0)])
            if row['units'] < 0 and row['tag'] != 'flow':
                tax = row['tax'] if row['tax_currency'] == 'BRL' else 0
                sales.append([row['tag'], row['security_type'], row['ticker'], row['currency'], row['calendar_date'], row['acquired_units'], avg_price, row['acquired_units'] * avg_price, row['final_estimate'], (avg_price - row['final_estimate']) * row['acquired_units'] - tax])
    
    print('Materializing results...')
    sales = pd.DataFrame(np.array(sales), columns=['tag', 'security_type', 'ticker', 'currency', 'calendar_date', 'units', 'avg_price', 'sale_amount', 'sale_price', 'profit'])
    mysql_lib.insert_df(sales, db['sales']['table'], db['sales']['columns'], merge=(len(db['sales']['columns']) > 0))

    df = mysql_lib.execute_query('queries/irpf/sales.sql')
    df.to_csv('csv/irpf/sales.csv', index=False)

    position = pd.DataFrame(np.array(avg_price_list), columns=['id', 'ticker', 'calendar_date', 'avg_price', 'units', 'position'])
    mysql_lib.insert_df(position, db['irpf']['table'], db['irpf']['columns'], merge=(len(db['irpf']['columns']) > 0))

    df = mysql_lib.execute_query('queries/irpf/irpf.sql')
    df.to_csv('csv/irpf/irpf.csv', index=False)


if __name__ == '__main__':
    print('Resetting tables...')
    reset_table('irpf')
    reset_table('sales')
    calculate_avg_prices()
    print('DONE')