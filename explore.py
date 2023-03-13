from lib.constants import *
from lib import data

import pandas as pd
import datetime as dt
import numpy as np

def unique_rows(df, key_col):
    counts = df.groupby(key_col).nunique()
    c = counts.max()
    if c.max() != 1:
        print(c)
    return c.max()

def concat_row(row, cols):
    res = ''
    for c in cols:
        v = row[c]
        if c == VALUE:
            v = round(v, 6)
        elif c == DATE and not isinstance(v, str):
            v = v.date()
        res += str(v)
    return res

def compare_join(df1, df2, keys, tag):
    print('=========== ' + tag + ' =========== ')
    df1[DATE] = df1[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%Y-%m-%d'))#.date())
    df1 = data.truncGroup(df1, True, keys, False)
    df1['Key'] = df1.apply(lambda x: concat_row(x, keys), axis=1)
    if unique_rows(df1, 'Key') != 1:
        print(df1)
        raise ValueError("Not unique")
    df1 = df1.set_index('Key')
    df1 = df1[keys + [VALUE]]

    df2[DATE] = df2[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%Y-%m-%d'))#.date())
    df2 = data.truncGroup(df2, True, keys, False)
    df2['Key'] = df2.apply(lambda x: concat_row(x, keys), axis=1)
    if unique_rows(df2, 'Key') != 1:
        print(df2)
        raise ValueError("Not unique")
    df2 = df2.set_index('Key')
    df2 = df2[keys + [VALUE]]

    sum_1 = round(df1[VALUE].sum(), 6)
    sum_2 = round(df2[VALUE].sum(), 6)
    print(sum_1)
    print(sum_2)

    if sum_1 != sum_2:
        income = df2.join(df1, how="outer", rsuffix='csv/irpf')
        print(income.reset_index(drop=True))
    
    return sum_1, sum_2

irpf = pd.read_csv('csv/irpf/income.csv')
up = pd.read_csv('debug/df_0.csv')
up = up.rename(columns={'Income': VALUE})
income = compare_join(irpf, up, [DATE], 'Income')


irpf = pd.read_csv('csv/irpf/expenses.csv')
up = pd.read_csv('debug/df_1.csv')
expenses = up.copy()
up = up.rename(columns={'Expenses': VALUE})
expense = compare_join(irpf, up, [SUBCATEGORY], 'Expenses')
# compare_join(irpf, up, [SUBCATEGORY, DATE], 'Expenses')

irpf = pd.read_csv('csv/irpf/exchange.csv')
exchange_irpf = irpf.copy()
irpf[SHARES] = irpf.apply(lambda x: -x[SHARES] if x[TYPE] == 'Sale' else x[SHARES], axis=1)
irpf = irpf.rename(columns={'Shares': VALUE})
up = pd.read_csv('debug/df_4.csv')
up = up.rename(columns={'Exchange': VALUE})
exchange = compare_join(irpf, up, [ACCOUNT, DATE], 'Exchange')

print('============== Buy tax ==============')
print(exchange_irpf[exchange_irpf[BUY_TAX_CURRENCY] == exchange_irpf[TICKER]][BUY_TAX].sum())
print(exchange_irpf[exchange_irpf[BUY_TAX_CURRENCY] == exchange_irpf[TICKER]])
print(expenses[expenses[SUBCATEGORY] == 'Buy tax'])

print('============== Total ==============')
print(income[0] - expense[0] + exchange[0])
print(income[1] - expense[1] + exchange[1])