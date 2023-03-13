from lib.constants import *
from lib.tools import *

import pandas as pd
import datetime as dt
import numpy as np
import threading
import re

# =============== BALANCE ============== #
def readFlow(name, currency, fillCol, truncate, groupBy, filterRemove, debug=False):
    df = read('csv/data/data_' + name + '.csv', currency, filterRemove)
    for col in fillCol:
        df[col] = df[col].fillna('')
    df[DATE] = df[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    df = truncGroup(df, truncate, groupBy, debug)
    return df


def calculateBalance(df, c, k, groupBy, balanceType):
    if df.empty:
        return df, pd.DataFrame([])
    if c == k:
        df = truncGroup(df, True, groupBy)
        return df, pd.DataFrame([])
    port = makePortfolio(df, c, k, balanceType)
    df[VALUE] = df.apply(lambda x: x[VALUE] * linearPrice(c, k, x[DATE]), axis=1)
    df = truncGroup(df, True, groupBy)
    df = df[[DATE, CATEGORY, SUBCATEGORY, VALUE]]
    port = computePortfolioIncome(port, c)
    return df, port


def readData(currency, truncate, filterRemove):
    # income
    income = readFlow('income', currency, [CATEGORY, SUBCATEGORY], truncate, [ACCOUNT, CATEGORY, SUBCATEGORY, DATE], filterRemove)
    income = income.rename(columns={VALUE: INCOME})

    # exchange
    exchange = readFlow('exchange', None, [], truncate, None, filterRemove)
    exIn = exchange.apply(lambda x: pd.Series([x[TICKER], x[SHARES], x[DATE], x[ACCOUNT]], index=[TICKER, VALUE, DATE, ACCOUNT]) if x[TYPE] == PURCHASE else pd.Series([x[CURRENCY], x[BUY] * x[SHARES], x[DATE], x[ACCOUNT]], index=[TICKER, VALUE, DATE, ACCOUNT]), axis=1)
    exOut = exchange.apply(lambda x: pd.Series([x[CURRENCY], -(x[BUY]*x[SHARES]), x[DATE], x[ACCOUNT]], index=[TICKER, VALUE, DATE, ACCOUNT])  if x[TYPE] == PURCHASE else pd.Series([x[TICKER], -x[SHARES], x[DATE], x[ACCOUNT]], index=[TICKER, VALUE, DATE, ACCOUNT]), axis=1)
    exchange = pd.concat([exIn, exOut])
    exchange = exchange[exchange[TICKER] == currency]
    exchange = exchange.rename(columns={VALUE: EXCHANGE})

    # expenses
    expenses = readFlow('expenses', currency, [CATEGORY, SUBCATEGORY], truncate, [ACCOUNT, CATEGORY, SUBCATEGORY, DATE], filterRemove)
    expenses = expenses.rename(columns={VALUE: EXPENSES})

    # tranfers
    transferOut = readFlow('transf', currency, [], truncate, [FROM, DATE], filterRemove)
    transferIn = readFlow('transf', currency, [], truncate, [TO, DATE], filterRemove)
    transferOut = transferOut.rename(columns={FROM: ACCOUNT})
    transferIn = transferIn.rename(columns={TO: ACCOUNT})
    transferOut = transferOut.rename(columns={VALUE: OUT})
    transferIn = transferIn.rename(columns={VALUE: IN})

    return income, expenses, transferOut, transferIn, exchange


# =============== EXCHANGE =============== #
def getExchange(targetCurrency, sourceCurrency, date, debug=False):
    prices = pd.read_csv('csv/data/data_prices.csv')
    date = (date - dt.timedelta(days=1)).strftime('%d/%m/%Y')
    
    if debug:
        print(targetCurrency)
        print(sourceCurrency)
        # print(prices[(prices[TICKER] == 'BRL')&(prices[CURRENCY] == 'USD')][date].item())

    fromSrc = prices[(prices[TICKER] == sourceCurrency)][CURRENCY].unique()

    if targetCurrency in fromSrc:
        return prices[(prices[TICKER] == sourceCurrency)&(prices[CURRENCY] == targetCurrency)][date].item()
    
    fromTgt = prices[(prices[TICKER] == targetCurrency)][CURRENCY].unique()

    if sourceCurrency in fromTgt:
        return 1 / prices[(prices[TICKER] == targetCurrency)&(prices[CURRENCY] == sourceCurrency)][date].item()
    
    inter = list(set(fromSrc) & set(fromTgt))

    if len(inter) > 0:
        intermediaryCurrency = inter[0]
        rateOne = prices[(prices[TICKER] == sourceCurrency)&(prices[CURRENCY] == intermediaryCurrency)][date].item()
        rateTwo = prices[(prices[TICKER] == targetCurrency)&(prices[CURRENCY] == intermediaryCurrency)][date].item()
        return rateOne / rateTwo
    
    toTgt = prices[(prices[CURRENCY] == targetCurrency)][TICKER].unique()
    inter = list(set(fromSrc) & set(toTgt))

    if len(inter) > 0:
        intermediaryCurrency = inter[0]
        rateOne = prices[(prices[TICKER] == sourceCurrency)&(prices[CURRENCY] == intermediaryCurrency)][date].item()
        rateTwo = prices[(prices[TICKER] == intermediaryCurrency)&(prices[CURRENCY] == targetCurrency)][date].item()
        
        if debug:
            print(sourceCurrency)
            print(intermediaryCurrency)
            print(targetCurrency)
            print(rateOne)
            print(rateTwo)

        return rateOne * rateTwo

    toSrc = prices[(prices[CURRENCY] == sourceCurrency)][TICKER].unique()
    inter = list(set(toSrc) & set(toTgt))

    if len(inter) > 0:
        intermediaryCurrency = inter[0]
        rateOne = prices[(prices[TICKER] == intermediaryCurrency)&(prices[CURRENCY] == sourceCurrency)][date].item()
        rateTwo = prices[(prices[TICKER] == intermediaryCurrency)&(prices[CURRENCY] == targetCurrency)][date].item()
        return rateTwo / rateOne
    
    inter = list(set(toSrc) & set(fromTgt))

    if len(inter) > 0:
        intermediaryCurrency = inter[0]
        rateOne = prices[(prices[TICKER] == intermediaryCurrency)&(prices[CURRENCY] == sourceCurrency)][date].item()
        rateTwo = prices[(prices[TICKER] == targetCurrency)&(prices[CURRENCY] == intermediaryCurrency)][date].item()
        return 1 / rateOne * rateTwo


def convertPrices(row, targetCurrency, originalTickers):
    if row[0] in originalTickers:
        row.iloc[0] = '-'
        return row
    for i, c in enumerate(row):
        if i > 1:
            # print('-----')
            # print(row[1])
            # print(targetCurrency)
            row.iloc[i] = c * getExchange(targetCurrency, row[1], dt.datetime.strptime(str(row.index[i]),'%d/%m/%Y') + dt.timedelta(days=1))
    row.iloc[1] = targetCurrency
    return row


def getConvertedPrices(targetCurrency):
    prices = pd.read_csv('csv/data/data_prices.csv')
    originalPrices = prices[prices[CURRENCY] == targetCurrency]
    originalTickers = originalPrices[TICKER].unique()
    convertedPrices = prices[prices[CURRENCY] != targetCurrency]
    try:
        convertedPrices = convertedPrices.apply(lambda x: convertPrices(x, targetCurrency, originalTickers), axis=1)
    except Exception as e:
        print('getConvertedPrices')
        print(e)
        print(targetCurrency)
        raise Exception
    convertedPrices = convertedPrices[convertedPrices[TICKER] != '-']
    prices = pd.concat([originalPrices, convertedPrices])
    return prices


def convertPortfolio(row, targetCurrency, debug=False):
    if debug:
        print('------------------')
        print(row.loc[CURRENCY])
        print(targetCurrency)
        print(getExchange(targetCurrency, row.loc[CURRENCY], dt.datetime(row[DATE].year, row[DATE].month, 1)))
        if getExchange(targetCurrency, row.loc[CURRENCY], dt.datetime(row[DATE].year, row[DATE].month, 1)) is None:
            getExchange(targetCurrency, row.loc[CURRENCY], dt.datetime(row[DATE].year, row[DATE].month, 1), True)
    row.loc[BUY] = row.loc[BUY] * getExchange(targetCurrency, row.loc[CURRENCY], dt.datetime(row[DATE].year, row[DATE].month, 1))
    row.loc[CURRENCY] = targetCurrency
    return row


def linearPrice(targetCurrency, sourceCurrency, d):
    y = getExchange(targetCurrency, sourceCurrency, getNextMonth(dt.datetime(d.year, d.month, 1)))
    y0 = getExchange(targetCurrency, sourceCurrency, dt.datetime(d.year, d.month, 1))
    m = (y - y0) / 30 
    return m * (d.day - 1) + y0


# =============== PORTFOLIO BALANCE =============== #
def computeIncome(row, prices, debug=False):
    previousMonth = ''
    # print(row)
    priceRow = prices[prices[TICKER] == row[TICKER]]
    for c in priceRow.columns:
        if c == TICKER or c == CURRENCY:
            continue
        date = dt.datetime.strptime(c, '%d/%m/%Y')#.date()
        if date < row[DATE]:
            row[c] = 0
        else:
            if previousMonth == '':
                try:
                    row[c] = (priceRow[c].item() - row[BUY]) * row[SHARES]
                except Exception as e:
                    print('---computeIncome---')
                    print(e)
                    print(c)
                    print(priceRow)
                    print(row)
                    raise Exception
                if debug:
                    prinT(c)
                    print(priceRow[c].item())
                    print(row[BUY])
                    # row[c] = priceRow[c].item() * row[SHARES]
                    print(row.to_frame().T)
            else:
                row[c] = (priceRow[c].item() - priceRow[previousMonth].item()) * row[SHARES]
                if debug:
                    prinT(c)
                    print(priceRow[c].item())
                    print(priceRow[previousMonth].item())
                    # row[c] = priceRow[c].item() * row[SHARES]
                    print(row.to_frame().T)
            previousMonth = c
    return row


def computePortfolioIncome(portfolio, targetCurrency, debug=False):
    prices = getConvertedPrices(targetCurrency)

    # portfolio = portfolio.apply(lambda x : computeIncome(x, prices, True), axis=1)
    portfolio = portfolio.apply(lambda x : computeIncome(x, prices), axis=1)
    months = list(prices.columns)
    months.pop(0)
    months.pop(0)
    cols = prices.columns.tolist()
    cols.pop(1)

    portfolioIncome = pd.melt(
        portfolio[cols], 
        id_vars=[TICKER], 
        value_vars=months,
        var_name=DATE, 
        value_name=VALUE
    )
    portfolioIncome = portfolioIncome.rename(columns={TICKER: NAME})
    portfolioIncome[CATEGORY] = portfolioIncome[NAME].apply(lambda x: mapIncomeCategory(x))
    portfolioIncome[SUBCATEGORY] = portfolioIncome[NAME]
    portfolioIncome[DATE] = portfolioIncome[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    portfolioIncome[DATE] = date_trunc(portfolioIncome[DATE], 'month')
    portfolioIncome.pop(NAME)
    portfolioIncome = pd.pivot_table(portfolioIncome, values=[VALUE], index=[DATE, CATEGORY, SUBCATEGORY], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    # # test = pd.pivot_table(portfolioIncome, values=[VALUE], index=[NAME, ACCOUNT, CATEGORY, SUBCATEGORY, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()

    return portfolioIncome[portfolioIncome[VALUE] != 0]


def makePortfolio(df, targetCurrency, sourceCurrency, portType):
    if df.empty:
        return df
    port = df[[VALUE, DATE]].copy()
    port[TICKER] = sourceCurrency
    port[BUY] = port.apply(lambda x: linearPrice(targetCurrency, sourceCurrency, x[DATE]), axis=1)
    port[SHARES] = port[VALUE] if portType == PURCHASE else -port[VALUE]
    port[BUY_TAX] = 0
    port[CURRENCY] = targetCurrency
    port[BUY_TAX_CURRENCY] = targetCurrency
    port = port[[TICKER, BUY, SHARES, BUY_TAX, DATE, CURRENCY, BUY_TAX_CURRENCY]].copy()
    return port


# =============== AUX =============== #        
def mapAllocationCategory(ticker):
    if ticker in set(['IRDM11', 'VNQ']):
        return REAL_STATE
    if ticker in set(['SAND', 'AXS', 'SLP', 'BCOIN', 'ATLAS', 'POLIS', 'THC', 'THG', 'FINA', 'RON']):
        return NFTG
    if ticker in set(['BTC', 'ETH', 'ADA', 'SOL', 'AVAX', 'BNB', 'DOT', 'MATIC', 'SHIB', 'SOL', 'LTC', 'LUNA2', 'LUNA']):
        return CRYPTO
    if ticker in set(['USDT', 'USDC', 'BUSD', 'GUSD']):
        return STABLECOIN
    if ticker in set(['BRAX11', 'BBSD11']):
        return BR_GROWTH
    if ticker in set(['TSLA34', 'TSLA']):
        return US_SUPER_GROWTH
    if ticker in set(['SXLU']):
        return US_VALUE
    if ticker in set(['JPMC34', 'MSBR34', 'MS']):
        return US_FINANCES
    if ticker in set(['BRL', 'EUR', 'USD']):
        return CASH
    if ticker in set(['PHPD']):
        return COMMOD
    if ticker in set(['Nubank', 'Savings', 'MTD', 'MTF']):
        return FIXED_INCOME 
    return US_GROWTH    


def mapIncomeCategory(ticker):
    if ticker in set(['SAND', 'AXS', 'SLP', 'BCOIN', 'ATLAS', 'POLIS', 'THC', 'THG', 'FINA', 'RON']):
        return NFTG
    if ticker in set(['LUNA', 'LUNA2', 'BTC', 'ETH', 'ADA', 'SOL', 'AVAX', 'BNB', 'DOT', 'MATIC', 'SHIB', 'SOL']):
        return CRYPTO
    if ticker in set(['USDT', 'USDC', 'BUSD', 'GUSD']):
        return STABLECOIN
    if ticker in set(['BRL', 'EUR', 'USD']):
        return CASH
    return INVESTMENTS 


def mapCategory(ticker, default):
    if ticker in set(['SAND', 'AXS', 'SLP', 'BCOIN', 'ATLAS', 'POLIS', 'THC', 'THG', 'FINA', 'RON']):
        return NFTG
    if ticker in set(['LUNA', 'LUNA2', 'BTC', 'ETH', 'ADA', 'SOL', 'AVAX', 'BNB', 'DOT', 'MATIC', 'SHIB', 'LTC']):
        return CRYPTO
    if ticker in set(['USDT', 'USDC', 'BUSD', 'GUSD']):
        return STABLECOIN
    if ticker in set(['Savings', 'TD SELIC L', 'TD Prefix L', 'TD IPCA L', 'CDB Prefix L', 'CDB IPCA L', 'CDB CDI L']):
        return SAVINGS
    if ticker in set(['CDB Prefix B', 'CDB IPCA B', 'CDB CDI B', 'TD Prefix B', 'TD IPCA B', 'TD SELIC B']):
        return BONDS        
    if ticker in set(['Nubank', 'TD SELIC E']):
        return EMERGENCY
    if ticker in set(['BRL', 'EUR', 'USD', 'Rivalry']):
        return CASH
    if ticker in set(['VNQ', 'O', 'IRDM', 'RBHY']):
        return REAL_STATE
    if ticker in set(['MS', 'JPMC', 'V', 'KO', 'SXLU', 'MTD', 'MTF', 'HDV']):
        return VALUE_STOCKS
    if ticker in set(['SPY', 'UST', 'AMZN', 'MSFT', 'META', 'GOOGL', 'TSLA', 'DIS', 'BRAX', 'RUS', 'PHPD']):
        return GROWTH_STOCKS
    return default 


def mapTickers(ticker):
    dictionary = {
        'MSBR34': 'MS',
        'JPMC34': 'JPMC',
        'AMZO34': 'AMZN',
        'DISB34': 'DIS',
        'GOGL34': 'GOOGL',
        'IRDM11': 'IRDM',
        'RBHY11': 'RBHY',
        'MSFT34': 'MSFT',
        'FBOK34': 'META',
        'IVVB11': 'SPY',
        'BRAX11': 'BRAX',
        'BBSD11': 'BRAX',
        'TSLA34': 'TSLA'
    }
    ticker_list = ticker.split(' ')
    if ticker_list[0] in ['CDB', 'TD']:
        bond_type = ''
        if 'E' in ticker_list:
            bond_type = ' E'
        elif 'L' in ticker_list:
            bond_type = ' L'
        else:
            bond_type = ' B'
        return ticker_list[0] + ' ' + ticker_list[1] + bond_type
    if ticker in dictionary.keys():
        return dictionary[ticker]
    return ticker 


def truncGroup(df, truncate, groupBy, debug=False):
    if truncate:
        df[DATE] = date_trunc(df[DATE], 'month')
    if groupBy is not None:
        # df[VALUE] = df[VALUE].apply(lambda x: float(x))
        df = pd.pivot_table(df, values=[VALUE], index=groupBy, aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    return df


# =============== VALIDATION =============== #
def filterAggregateBalances(dfList, date, mode='account'):
    filterDate = dt.datetime.strptime(str(date),'%d/%m/%Y')#.date()
    joinedDf = pd.DataFrame({'A' : []})
    # df_type = {
    #     0: 'Income',
    #     1: 'Expense',
    #     2: 'Out',
    #     3: 'In',
    #     4: 'Exchange'

    # }
    for count, df in enumerate(dfList):
        if df.empty:
            continue
        df = df[df[DATE] <= filterDate]
        if mode == 'account':
            df = pd.pivot_table(df, values=[COLS[count]], index=[ACCOUNT], aggfunc={COLS[count]: np.sum}, fill_value=0)#.reset_index()
        else:
            df = pd.pivot_table(df, values=[COLS[count]], index=[DATE], aggfunc={COLS[count]: np.sum}, fill_value=0)#.reset_index()
        joinedDf = df if joinedDf.empty else joinedDf.join(df, how="outer")
        joinedDf = joinedDf.fillna(0)

        try:
            if not(VALUE in joinedDf.columns):
                if count == 0 or count == 3 or count == 4:
                    joinedDf[VALUE] = joinedDf[COLS[count]]
                else:
                    joinedDf[VALUE] = -joinedDf[COLS[count]]
            elif count == 3 or count == 4:
                joinedDf[VALUE] += joinedDf[COLS[count]]
            else:
                joinedDf[VALUE] -= joinedDf[COLS[count]]
        except:
            count += 1
    
    # return []
    return joinedDf


def validateData(dict, currency, date, printMsg):
    df = filterAggregateBalances(dict[currency], date)
    # print(df)
    df[VALUE] = df[VALUE].apply(lambda x: round(x, 6))

    balances = read('csv/data/data_balances.csv', currency, False)
    balances[DATE] = balances[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    balances = balances[balances[DATE] == dt.datetime.strptime(str(date),'%d/%m/%Y')].set_index(ACCOUNT)
    diff = balances[VALUE] - df[VALUE]
    diff = diff.fillna(0)
    diff = diff.apply(lambda x: round(x, 4))
    if diff.sum() == 0 and balances.size != 0:
        return True, df
    else:
        if printMsg:
            print('-------------------ERROR-------------------')
            print(df)
            print(balances)
            print(diff)
        return False, df


# =============== ALLOCATION =============== #        
def readAllocation(currency, endDate):
    print(currency)
    dfAlloc = pd.DataFrame({TICKER: [], DATE: [], POSITION: []})
    dfAllocDaily = pd.DataFrame({TICKER: [], DATE: [], AVG_POSITION: []})
    # dfAlloc = pd.DataFrame({TICKER: [], DATE: [], POSITION: [], FLEX_POSITION: [], FLEX_INCOME: []})
    dfList = readData(currency, truncate=False, filterRemove=True) 
    months = generateDates(dt.datetime(2019, 8, 1), dt.datetime.strptime(endDate,'%d/%m/%Y'))
    dates = generateDates(dt.datetime(2019, 8, 1), dt.datetime.strptime(endDate,'%d/%m/%Y'), mode='daily')
    # dfListFlex = []
    df = filterAggregateBalances(dfList, endDate, mode='monthly').reset_index()
    for d in dates:
        temp = df[df[DATE] <= d]
        dfAllocDaily.loc[-1] = [currency, d, temp[VALUE].sum() if not temp.empty else 0]
        dfAllocDaily = dfAllocDaily.reset_index(drop=True)
    dfAllocDaily[DATE] = date_trunc(dfAllocDaily[DATE])
    dfAllocDaily = pd.pivot_table(dfAllocDaily, values=[AVG_POSITION], index=[TICKER, DATE], aggfunc={AVG_POSITION: np.average}, fill_value=0).reset_index()
    # dfAllocDaily = dfAllocDaily.set_index([TICKER, DATE])
    for df in dfList:
        # dfListFlex.append(df.copy())
        df[DATE] = date_trunc(df[DATE])
    # for df in dfListFlex:
    #     df[DATE] = date_trunc(df[DATE].apply(lambda x: getFlexDate(x)))
    # print(dfListFlex[0])
    df = filterAggregateBalances(dfList, endDate, mode='monthly').reset_index()
    for d in months:
        temp = df[df[DATE] <= d]
        # df = filterAggregateBalances(dfList, d.strftime('%d/%m/%Y'), mode='monthly')
        # dfFlex = filterAggregateBalances(dfListFlex, d.strftime('%d/%m/%Y'))
        dfAlloc.loc[-1] = [currency, d, temp[VALUE].sum() if not temp.empty else 0]
        # dfAlloc.loc[-1] = [currency, d, df[VALUE].sum() if not df.empty else 0, dfFlex[VALUE].sum() if not dfFlex.empty else 0, None]
        dfAlloc = dfAlloc.reset_index(drop=True)
    dfAlloc = dfAlloc.set_index([TICKER, DATE])
    dfAllocDaily = dfAllocDaily.set_index([TICKER, DATE])
    dfAlloc = dfAlloc.join(dfAllocDaily, how="outer").reset_index()
    return dfAlloc
    # return []
        


# =============== OTHER =============== #        
def generateDates(start, end, mode='monthly'):
    dates = []
    while True:
        dates.append(start)
        if mode == 'monthly':
            start = getNextMonth(start)
        else:
            start = getNextDay(start)
        if start > end:
            break
    return dates        


def getFlexDate(date):
    if date.day >= DATE_TRESHOLD:
        date = date.replace(day=1, month=date.month + 1 if date.month < 12 else 1, year=date.year if date.month < 12 else date.year + 1)
    return date


def computeFlexibleIncome(row, prices):
    previousMonth = ''
    priceRow = prices[prices[TICKER] == row[TICKER]]
    for c in priceRow.columns:
        if c == TICKER:
            continue
        date = dt.datetime.strptime(c, '%d/%m/%Y')#.date()
        early_begining = date.year == row[DATE].year and date.month == row[DATE].month and row[DATE].day < DATE_TRESHOLD
        late_begining_1 = date.year == row[DATE].year and date.month - 1 == row[DATE].month and row[DATE].day >= DATE_TRESHOLD
        late_begining_2 = date.year - 1 == row[DATE].year and date.month == 1 and row[DATE].month == 12 and row[DATE].day >= DATE_TRESHOLD
        if early_begining or late_begining_1 or late_begining_2:
            row[c] = (priceRow[c].item() - row[BUY]) * row[SHARES]
            previousMonth = c
        elif date < row[DATE]:
            row[c] = 0
        else:
            if previousMonth == '':
                row[c] = 0
            else:
                row[c] = (priceRow[c].item() - priceRow[previousMonth].item()) * row[SHARES]
            previousMonth = c
    return row


def computePositions(row, prices):
    priceRow = prices[prices[TICKER] == row[TICKER]]
    for c in priceRow.columns:
        if c == TICKER:
            continue
        date = dt.datetime.strptime(c, '%d/%m/%Y')#.date()
        if date < row[DATE]:
            row[c] = 0
        else:
            row[c] = priceRow[c].item() * row[SHARES]
    return row    


def computeFlexiblePositions(row, prices):
    priceRow = prices[prices[TICKER] == row[TICKER]]
    for c in priceRow.columns:
        if c == TICKER:
            continue
        date = dt.datetime.strptime(c, '%d/%m/%Y')#.date()
        if date.year == row[DATE].year and date.month == row[DATE].month and row[DATE].day < DATE_TRESHOLD:
            row[c] = priceRow[c].item() * row[SHARES]
        elif (date.year == row[DATE].year and date.month == row[DATE].month and row[DATE].day >= DATE_TRESHOLD) or date <= row[DATE]:
            row[c] = 0
        else:
            row[c] = priceRow[c].item() * row[SHARES]
    return row    


def processPortfolio(portfolio, prices, function, valueColumn):
    months = list(prices.columns)
    months.pop(0)

    tempPortfolio = portfolio.apply(lambda x : function(x, prices), axis=1)
    tempPortfolio.to_csv('test/1.csv')
    tempPortfolio = pd.melt(
        tempPortfolio[prices.columns], 
        id_vars=[TICKER], 
        value_vars=months,
        var_name=DATE, 
        value_name=VALUE
    )
    tempPortfolio = tempPortfolio.rename(columns={TICKER: NAME})
    tempPortfolio[CATEGORY] = tempPortfolio[NAME].apply(lambda x: mapAllocationCategory(x))
    tempPortfolio[SUBCATEGORY] = tempPortfolio[NAME]
    tempPortfolio.to_csv('test/2.csv')
    tempPortfolio = pd.pivot_table(tempPortfolio, values=[VALUE], index=[NAME, CATEGORY, SUBCATEGORY, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    tempPortfolio = tempPortfolio.rename(columns={VALUE: valueColumn})
    tempPortfolio.set_index([NAME, CATEGORY, SUBCATEGORY, DATE])
    tempPortfolio.to_csv('test/3.csv')

    return tempPortfolio


# =============== MAIN =============== #

def readNU():
    # income
    income = read('csv/data/data_income.csv', True)
    income[CATEGORY] = income[CATEGORY].fillna('')
    income[SUBCATEGORY] = income[SUBCATEGORY].fillna('')
    income = income[income[ACCOUNT] == NU]
    income[DATE] = income[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())

    flexIncome = income.copy()
    flexIncome[DATE] = flexIncome[DATE].apply(lambda x : getFlexDate(x))
    flexIncome[DATE] = date_trunc(flexIncome[DATE], 'month')

    income[DATE] = date_trunc(income[DATE], 'month')
    income = pd.pivot_table(income, values=[VALUE], index=[DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    income = income.rename(columns={VALUE: INCOME})

    yieldIncome = flexIncome[flexIncome[SUBCATEGORY] == FIXED_INCOME]
    yieldIncome = pd.pivot_table(yieldIncome, values=[VALUE], index=[DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    yieldIncome = yieldIncome.rename(columns={VALUE: FLEX_INCOME})
    
    flexIncome = pd.pivot_table(flexIncome, values=[VALUE], index=[DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    flexIncome = flexIncome.rename(columns={VALUE: INCOME})

    # expenses
    expenses = read('csv/data/data_expenses.csv', True)
    expenses = expenses[expenses[ACCOUNT] == NU]
    expenses[CATEGORY] = expenses[CATEGORY].fillna('')
    expenses[SUBCATEGORY] = expenses[SUBCATEGORY].fillna('')
    expenses[DATE] = expenses[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    
    flexExpenses = expenses.copy()
    flexExpenses[DATE] = flexExpenses[DATE].apply(lambda x : getFlexDate(x))
    flexExpenses[DATE] = date_trunc(flexExpenses[DATE], 'month')
    flexExpenses = pd.pivot_table(flexExpenses, values=[VALUE], index=[DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    flexExpenses = flexExpenses.rename(columns={VALUE: EXPENSES})

    expenses[DATE] = date_trunc(expenses[DATE], 'month')
    expenses = pd.pivot_table(expenses, values=[VALUE], index=[DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    expenses = expenses.rename(columns={VALUE: EXPENSES})

    # tranfers
    transfer = read('csv/data/data_transf.csv', True)
    transfer = transfer[(transfer[FROM] == NU) | (transfer[TO] == NU)]
    transfer[DATE] = transfer[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())

    flexTransfer = transfer.copy()
    flexTransfer[DATE] = flexTransfer[DATE].apply(lambda x : getFlexDate(x))
    flexTransfer[DATE] = date_trunc(flexTransfer[DATE], 'month')
    flexTransferOut = pd.pivot_table(flexTransfer, values=[VALUE], index=[FROM, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    flexTransferOut = flexTransferOut[flexTransferOut[FROM] == NU]
    flexTransferIn = pd.pivot_table(flexTransfer, values=[VALUE], index=[TO, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    flexTransferIn = flexTransferIn[flexTransferIn[TO] == NU]

    flexTransferOut = flexTransferOut.rename(columns={VALUE: 'Out'})
    flexTransferIn = flexTransferIn.rename(columns={VALUE: 'In'})

    transfer[DATE] = date_trunc(transfer[DATE], 'month')
    transferOut = pd.pivot_table(transfer, values=[VALUE], index=[FROM, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    transferOut = transferOut[transferOut[FROM] == NU]
    transferOut = transferOut[[DATE, VALUE]]

    transferIn = pd.pivot_table(transfer, values=[VALUE], index=[TO, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    transferIn = transferIn[transferIn[TO] == NU]
    transferIn = transferIn[[DATE, VALUE]]

    transferOut = transferOut.rename(columns={VALUE: 'Out'})
    transferIn = transferIn.rename(columns={VALUE: 'In'})

    # consolidated
    df = income.set_index(DATE).join(expenses.set_index(DATE), rsuffix='_2').join(transferOut.set_index(DATE), rsuffix='_3').join(transferIn.set_index(DATE), rsuffix='_4', how='outer')
    df = df.fillna(0)
    df[POSITION] = df[INCOME] - df[EXPENSES] - df['Out'] + df['In']
    df[POSITION] = df[POSITION].cumsum()
    df = df.reset_index()
    df = df[[DATE, POSITION]]

    flexDf = flexIncome.set_index(DATE).join(flexExpenses.set_index(DATE), rsuffix='_2').join(flexTransferOut.set_index(DATE), rsuffix='_3').join(flexTransferIn.set_index(DATE), rsuffix='_4')
    flexDf = flexDf.fillna(0)
    flexDf[FLEX_POSITION] = flexDf[INCOME] - flexDf[EXPENSES] - flexDf['Out'] + flexDf['In']
    flexDf[FLEX_POSITION] = flexDf[FLEX_POSITION].cumsum()
    flexDf = flexDf.reset_index()
    flexDf = flexDf[[DATE, FLEX_POSITION]]

    resultDf = df.set_index(DATE).join(flexDf.set_index(DATE), rsuffix='_2').join(yieldIncome.set_index(DATE), rsuffix='_3')
    resultDf = resultDf.reset_index()
    resultDf = resultDf[[DATE, POSITION, FLEX_POSITION, FLEX_INCOME]]
    resultDf[CATEGORY] = FIXED_INCOME
    resultDf[SUBCATEGORY] = NU

    return resultDf


def readPortfolio():
    portfolio = read('csv/data/data_portfolio_buy.csv', True)
    portfolio[DATE] = portfolio[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())

    portfolioSell = read('csv/data/data_portfolio_sell.csv', True)
    portfolioSell[DATE] = portfolioSell[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    portfolioSell[SHARES] = -portfolioSell[SHARES]  
    portfolio = pd.concat([portfolio, portfolioSell])

    prices = pd.read_csv('csv/data/data_prices.csv')

    portfolioFlexibleIncome = processPortfolio(portfolio, prices, computeFlexibleIncome, FLEX_INCOME)
    portfolioFlexPositions = processPortfolio(portfolio, prices, computeFlexiblePositions, FLEX_POSITION)
    portfolioPositions = processPortfolio(portfolio, prices, computePositions, POSITION)

    portfolio = portfolioPositions.join(portfolioFlexibleIncome, rsuffix='_2').join(portfolioFlexPositions, rsuffix='_3')
    portfolio = portfolio[[CATEGORY, SUBCATEGORY, DATE, POSITION, FLEX_POSITION, FLEX_INCOME]]
    portfolio = portfolio[(portfolio[POSITION] != 0) | (portfolio[FLEX_INCOME] != 0) | (portfolio[FLEX_POSITION] != 0)]
    portfolio[DATE] = portfolio[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    portfolio[DATE] = date_trunc(portfolio[DATE], 'month')


    return portfolio


if __name__ == '__main__':
    print('Hi')