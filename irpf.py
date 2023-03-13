from lib import data
from lib.tools import *
from lib import mongo
from lib.constants import *
import pandas as pd
import datetime as dt
import numpy as np


def convert(exchange, c='BRL'):
    exchange[ORIGINAL_CURRENCY] = exchange[CURRENCY]
    
    originalExchange = exchange[exchange[CURRENCY] == c].copy()
    convertedExchange = exchange[exchange[CURRENCY] != c].copy()
    
    convertedExchange['Test 0'] = convertedExchange.apply(lambda x: x[BUY] * data.getExchange(c, x[ORIGINAL_CURRENCY], x[DATE]), axis=1)
    convertedExchangeTemp = convertedExchange.apply(lambda x: data.convertPortfolio(x, c), axis=1)

    convertedExchangeTemp['Test'] = convertedExchangeTemp.apply(lambda x: data.getExchange(c, x[TICKER], x[DATE]), axis=1)
    convertedExchangeTemp['Diff'] = (convertedExchangeTemp['Test'] - convertedExchangeTemp['Buy']) / convertedExchangeTemp['Buy']
    convertedExchangeTemp['Test 2'] = convertedExchangeTemp.apply(lambda x: data.getExchange(c, x[TICKER], getNextMonth(x[DATE])), axis=1)
    convertedExchangeTemp['Diff 2'] = (convertedExchangeTemp['Test 2'] - convertedExchangeTemp['Buy']) / convertedExchangeTemp['Buy']
    convertedExchangeTemp[BUY] = convertedExchangeTemp.apply(lambda x: x['Test 0'] if x[ORIGINAL_CURRENCY] in ['USDT', 'BUSD', 'USDC', 'USD', 'BRL', 'EUR'] else (x['Test'] if abs(x['Diff']) > 0.3 and abs(x['Diff 2']) > 0.3 or x[TICKER] in ['USDT', 'BUSD', 'USDC', 'USD', 'BRL', 'EUR'] else x[BUY]), axis=1)
    convertedExchange = convertedExchangeTemp
    convertedExchange.pop('Test 0')
    convertedExchange.pop('Test')
    convertedExchange.pop('Test 2')
    convertedExchange.pop('Diff')
    convertedExchange.pop('Diff 2')

    exchange = pd.concat([originalExchange, convertedExchange])
    return exchange


def revert(row):
    buy = 1 / row[BUY]
    shares = row[BUY] * row[SHARES]
    ticker = row[CURRENCY]
    currency = row[TICKER]
    row[TYPE] = 'Purchase' if row[TYPE] == 'Sale' else 'Sale'
    row[BUY] = buy
    row[SHARES] = shares
    row[TICKER] = ticker
    row[CURRENCY] = currency
    return row


def securityType(ticker):
    if ticker in set(['BTC', 'ETH', 'ADA', 'SOL', 'AVAX', 'BNB', 'DOT', 'MATIC', 'SAND', 'SHIB', 'SOL', 'LTC', 'LUNA', 
        'AXS', 'SLP', 'BCOIN', 'ATLAS', 'POLIS', 'THC', 'THG', 'FINA', 'RON', 'USDT', 'USDC', 'BUSD']):
        return 'Crypto'
    if ticker in set(['IRDM11', 'RBHY']):
        return 'FII'
    if ticker in set(['BRAX11', 'BBSD11', 'IVVB11']):
        return 'ETF'
    if ticker in set(['AMZO34', 'BBSD11', 'BRAX11', 'DISB34', 'FBOK34', 'GOGL34', 'JPMC34', 'MSBR34', 'MSFT34', 'TSLA34']):
        return 'BDR'
    if ticker in set(['SPY', 'UST', 'AMZN', 'MSFT', 'META', 'GOOGL', 'TSLA', 'MS', 'JPMC', 'V', 'KO', 'VNQ', 'O']):
        return 'Stocks'
    return 'Other'


def calculatePositions(ticker, filterDate):
    df = data.readFlow('exchange', None, [], truncate=False, groupBy=None, filterRemove=False)
    dfIncome = data.readFlow('income', None, [], truncate=False, groupBy=None, filterRemove=False)
    dfExpenses = data.readFlow('expenses', None, [], truncate=False, groupBy=None, filterRemove=False)

    df = df[df[DATE] <= filterDate]
    # df = df[df[CURRENCY] != 'EUR']
    dfReverse = df[df[CURRENCY] == ticker].copy()
    df = df[df[TICKER] == ticker]
    
    # print('-- Ticker --')
    # print(df)

    dfReverse = dfReverse.apply(lambda x: revert(x), axis=1)

    # print('-- Reverse --')
    # print(dfReverse)

    dfIncome = dfIncome[dfIncome[CURRENCY] == ticker]
    dfIncome = dfIncome[dfIncome[DATE] <= filterDate]
    dfExpenses = dfExpenses[dfExpenses[CURRENCY] == ticker]
    dfExpenses = dfExpenses[dfExpenses[SUBCATEGORY] != 'Buy tax']
    dfExpenses = dfExpenses[dfExpenses[DATE] <= filterDate]

    # print('-- Income --')
    # print(dfIncome)
    # print('-- Expense --')
    # print(dfExpenses)
    dfIncome.to_csv('csv/irpf/income.csv', index=False)
    dfExpenses.to_csv('csv/irpf/expenses.csv', index=False)
    
    df = pd.concat([df, dfReverse])
    df.to_csv('csv/irpf/exchange.csv', index=False)
    originalDate = df[DATE].copy()
    df[DATE] = date_trunc(df[DATE], 'month')

    # print('--------------------------------------')
    # # print('Purchase: ', df[TYPE])
    # print('Purchase: ', df[df[TYPE] == 'Purchase'][SHARES].sum())
    # print('Sale: ', df[df[TYPE] == 'Sale'][SHARES].sum())
    # print('Income: ', dfIncome[VALUE].sum())
    # print('Expense: ', dfExpenses[VALUE].sum())
    # print('Total: ', df[df[TYPE] == 'Purchase'][SHARES].sum() 
    #     - df[df[TYPE] == 'Sale'][SHARES].sum() 
    #     + dfIncome[VALUE].sum() 
    #     - dfExpenses[VALUE].sum())
    # print('--------------------------------------')

    if len(df[df[CURRENCY] != 'BRL'].index) > 0:
        df = convert(df)
        df[DATE] = originalDate
        df = pd.concat([df, pd.DataFrame(np.array([[ticker, '', 0, dfIncome[VALUE].sum(), 0, filterDate, 'Purchase', 'BRL', 'BRL', 'BRL']]), columns=list(df.columns))])
        df = pd.concat([df, pd.DataFrame(np.array([[ticker, '', 0, dfExpenses[VALUE].sum(), 0, filterDate, 'Sale', 'BRL', 'BRL', 'BRL']]), columns=list(df.columns))])
    else:
        df[DATE] = originalDate
        df = pd.concat([df, pd.DataFrame(np.array([[ticker, '', 0, dfIncome[VALUE].sum(), 0, filterDate, 'Purchase', 'BRL', 'BRL']]), columns=list(df.columns))])
        df = pd.concat([df, pd.DataFrame(np.array([[ticker, '', 0, dfExpenses[VALUE].sum(), 0, filterDate, 'Sale', 'BRL', 'BRL']]), columns=list(df.columns))])


    df = df.sort_values([DATE,  TYPE, ACCOUNT], ascending=[True, True, False])
    # print(df)


    sales = None
    avgPrice = 0
    units = 0
    for index, row in df.iterrows():
        # print('----------------------------------------------------------------------------')
        if row[TYPE] == 'Purchase':
            acquisitionValue = 0
            acquiredUnits = 0
            if row[CURRENCY] == row[BUY_TAX_CURRENCY] and row[CURRENCY] == 'BRL':
                acquisitionValue = row[BUY] * row[SHARES] + row[BUY_TAX]
                acquiredUnits = row[SHARES]
            elif row[TICKER] == row[BUY_TAX_CURRENCY]:
                acquisitionValue = row[BUY] * row[SHARES]
                acquiredUnits = row[SHARES] - row[BUY_TAX]
            elif row[BUY_TAX_CURRENCY] == 'BNB':
                acquisitionValue = row[BUY] * row[SHARES] * 1.0075
                acquiredUnits = row[SHARES]
            else:
                acquisitionValue = row[BUY] * row[SHARES]
                acquiredUnits = row[SHARES]
            # print('--------------------------------------')
            avgPrice = (avgPrice * units + acquisitionValue) / (units + acquiredUnits)
            units += acquiredUnits
        elif row[TYPE] == 'Sale':
            newSale = pd.DataFrame(np.array([[
                ticker, dt.datetime(row[DATE].year, row[DATE].month, 1), avgPrice, row[BUY], row[SHARES], (row[BUY] - avgPrice)*row[SHARES], row[BUY]*row[SHARES], securityType(ticker)]]), 
                columns=['Ticker', 'Month', 'Avg Price', 'Sell Price', 'Sold Units', 'Profit', 'Volume', 'Type'])
            if sales is None and row[BUY] != 0:
                sales = newSale
            elif row[BUY] != 0:
                sales = pd.concat([sales, newSale])

            units -= row[SHARES]
        else:
            print(row[TYPE])
            print('?')
        # print(ticker)
        # print('Acq. Price: ', acquisitionValue)
        # print('Acq. Units     : ', acquiredUnits)
        # print('Row  : ', row[BUY], ' - ', row[SHARES], ' - ', row[BUY_TAX])
        # print('--------------------------------------')
        # print(ticker)
        # print('Avg. Price: ', avgPrice)
        # print('Units     : ', units)
        # print('Position  : ', avgPrice * units)
        # print('--------------------------------------')
    return pd.DataFrame(np.array([[ticker, avgPrice, units, avgPrice * units]]), columns=['Ticker', 'Avg. Price', 'Units', 'Position']), sales



filterDate = dt.datetime.strptime('31/12/2023','%d/%m/%Y')#.date()
# filterDate = dt.datetime.strptime('31/12/2022','%d/%m/%Y')#.date()
df = None
s = None

for t in ['AMZO34', 'BBSD11', 'BRAX11', 'DISB34', 'FBOK34', 'GOGL34', 'IRDM11', 'RBHY', 
    'IVVB11', 'JPMC34', 'MSBR34', 'MSFT34', 'TSLA34', 'BTC', 'ETH',
    'AVAX', 'BNB', 'MATIC', 'SAND', 'SHIB', 'BUSD', 'USDT', 'AXS', 'POLIS', 'USDC', 'BNB',
    'SPY', 'MSFT', 'TSLA', 'MS', 'V', 'KO', 'VNQ', 'O', 'HDV']:
# for t in ['AVAX']:
    print('--', t, '--')
    dfi, si = calculatePositions(t, filterDate)
    df = dfi if df is None else pd.concat([df, dfi])
    s = si if s is None else pd.concat([s, si])
print(df)
print(s)
df.to_csv('csv/irpf/positions_eur.csv', index=False)
s.to_csv('csv/irpf/sales.csv', index=False)
# df = data.readFlow('balances', None, [], truncate=False, groupBy=None, filterRemove=False)
# print(df[df[DATE] == dt.datetime.strptime('27/12/2021','%d/%m/%Y')][CURRENCY].unique())