from lib import data
from lib.tools import *
from lib.constants import *
import pandas as pd
import datetime as dt
import numpy as np
import threading
from time import sleep, time

DEBUG = False
DEBUG_CURRENCY = 'USDC'
# CURRENT_DATE = '31/08/2022'
CURRENT_DATE = '28/02/2023'

# =============== THREADING ============== #

# class validationThread(threading.Thread):

#     def __init__(self, currency, outputs, validated):
#         self.currency = currency
#         self.outputs = outputs
#         self.validated = validated
#         super(validationThread, self).__init__()
    
#     def validate(self):
#         self.outputs[self.currency] = data.readData(self.currency, truncate=True, filterRemove=False)
#         self.validated, df = data.validateData(self.outputs, self.currency, CURRENT_DATE, True)

#     def run(self):
#         print('\nRunning validation - ' + self.currency)
#         self.validate()

def validateRawData(currencies):
    outputs = {}
    validated = True
    # for c in ['USD']:
    for c in currencies:
        print(c)
        outputs[c] = data.readData(c, truncate=True, filterRemove=False)
        v, df = data.validateData(outputs, c, CURRENT_DATE, True)
        validated = validated and v
    return validated
    # return validated, outputs.keys()

   
   
def calculateRawBalance(currencies):
    outputs = {}
    realBalance = pd.DataFrame({TICKER : [], VALUE : []})
    for c in currencies:
        # print(c)
        outputs[c] = data.readData(c, truncate=True, filterRemove=True)
        temp, df = data.validateData(outputs, c, CURRENT_DATE, False)
        active = df.sum()

        realBalance.loc[-1] = [c, active[VALUE]]
        realBalance = realBalance.reset_index(drop=True)
    return realBalance


def calculateCurrencyBalance(c, calculatedBalance, realBalance):
    prices = data.getConvertedPrices(c)
    prices[PRICE] = prices.iloc[:,-1:]
    prices = prices[[TICKER, PRICE]]

    realBalance = realBalance.join(prices.set_index(TICKER), on=TICKER) 
    realBalance[TOTAL] = realBalance[VALUE] * realBalance[PRICE]                
    realBalance[SHARES] = realBalance[VALUE]               
    realBalance = realBalance[[TICKER, SHARES, TOTAL]].join(calculatedBalance.set_index(TICKER), on=TICKER)
    realBalance['Diff'] = realBalance[VALUE] - realBalance[TOTAL]
    realBalance['Diff'] = realBalance['Diff'].apply(lambda x: round(x, 6))

    return realBalance


def calculateExchangeIncome(c, flex=False, checkValues=True):
    if flex:
        exchange = data.readFlow('exchange', None, [], truncate=False, groupBy=None, filterRemove=True)
        exchange[DATE] = exchange[DATE].apply(lambda x: data.getFlexDate(x))
        exchange[DATE] = date_trunc(exchange[DATE], 'month')
    else:
        exchange = data.readFlow('exchange', None, [], truncate=True, groupBy=None, filterRemove=True)
    
    if DEBUG:
        exchange = exchange[(exchange[TICKER] == DEBUG_CURRENCY)|(exchange[CURRENCY] == DEBUG_CURRENCY)]
    
    exchange[ORIGINAL_CURRENCY] = exchange[CURRENCY]
    
    originalExchange = exchange[exchange[CURRENCY] == c].copy()
    convertedExchange = exchange[exchange[CURRENCY] != c].copy()

    if convertedExchange.size > 0:
        # convertedExchange['Test 0'] = convertedExchange.apply(lambda x: a(c, x), axis=1)
        convertedExchange['Test 0'] = convertedExchange.apply(lambda x: x[BUY] * data.getExchange(c, x[ORIGINAL_CURRENCY], x[DATE]), axis=1)
        convertedExchangeTemp = convertedExchange.apply(lambda x: data.convertPortfolio(x, c), axis=1)

        convertedExchangeTemp['Test'] = convertedExchangeTemp.apply(lambda x: data.getExchange(c, x[TICKER], x[DATE]), axis=1)
        convertedExchangeTemp['Diff'] = (convertedExchangeTemp['Test'] - convertedExchangeTemp['Buy']) / convertedExchangeTemp['Buy']
        convertedExchangeTemp['Test 2'] = convertedExchangeTemp.apply(lambda x: data.getExchange(c, x[TICKER], getNextMonth(x[DATE])), axis=1)
        convertedExchangeTemp['Diff 2'] = (convertedExchangeTemp['Test 2'] - convertedExchangeTemp['Buy']) / convertedExchangeTemp['Buy']
        convertedExchangeTemp[BUY] = convertedExchangeTemp.apply(lambda x: x['Test 0'] if x[ORIGINAL_CURRENCY] in ['USDT', 'BUSD', 'USDC', 'USD', 'BRL', 'EUR'] else (x['Test'] if abs(x['Diff']) > 0.3 and abs(x['Diff 2']) > 0.3 or x[TICKER] in ['USDT', 'BUSD', 'USDC', 'USD', 'BRL', 'EUR'] else x[BUY]), axis=1)
        convertedExchange = convertedExchangeTemp

    if DEBUG:
        print(convertedExchange[[TICKER, BUY, SHARES, DATE, CURRENCY, ORIGINAL_CURRENCY, 'Test 0', 'Test', 'Diff', 'Test 2', 'Diff 2']])

    print('- Deport compensation')
    deportCompensation = exchange.copy()
    deportCompensation[TICKER] = exchange[CURRENCY] 
    deportCompensation[CURRENCY] = exchange[TICKER]
    deportCompensation[SHARES] = exchange[BUY] * exchange[SHARES]
    deportCompensation[ORIGINAL_CURRENCY] = exchange[TICKER]
    deportCompensation[TYPE] = deportCompensation[TYPE].apply(lambda x: PURCHASE if x == SALE else SALE)

    deportCompensation[CURRENCY] = c
    if convertedExchange.size > 0:
        deportCompensation = deportCompensation.join(convertedExchange[[BUY]], rsuffix='Converted').join(exchange[[BUY]], rsuffix='Original')
        deportCompensation[BUY] = deportCompensation.apply(lambda x: 1 if pd.isna(x['BuyConverted']) else x['BuyConverted'] / x['BuyOriginal'], axis=1)
    
    if convertedExchange.size > 0:
        exchange = pd.concat([originalExchange, convertedExchange])

    if DEBUG:
        print(deportCompensation)
        print(exchange)
    
    if checkValues:
        print('- Aport')
        aport = exchange[[TICKER, BUY, SHARES, TYPE, ORIGINAL_CURRENCY]].copy()
        aport[VALUE] = aport[BUY] * aport[SHARES]
        aport[VALUE] = aport.apply(lambda x: x[VALUE] if x[TYPE] == PURCHASE else -x[VALUE], axis=1)
        aportPair = pd.pivot_table(aport, values=[VALUE], index=[TICKER, ORIGINAL_CURRENCY], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
        aport = pd.pivot_table(aport, values=[VALUE], index=[TICKER], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
        
        print('- Deport')
        deport = deportCompensation[[TICKER, BUY, SHARES, TYPE, ORIGINAL_CURRENCY]].copy()
        deport[VALUE] = deport[BUY] * deport[SHARES]
        deport[VALUE] = deport.apply(lambda x: x[VALUE] if x[TYPE] == PURCHASE else -x[VALUE], axis=1)
        deportPair = pd.pivot_table(deport, values=[VALUE], index=[TICKER, ORIGINAL_CURRENCY], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
        deport = pd.pivot_table(deport, values=[VALUE], index=[TICKER], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()

        deportPair = deportPair.rename(columns={TICKER: ORIGINAL_CURRENCY, ORIGINAL_CURRENCY: TICKER})
        diff = aportPair.set_index([TICKER, ORIGINAL_CURRENCY]).join(deportPair.set_index([TICKER, ORIGINAL_CURRENCY]), rsuffix='_deport')
        diff['Diff'] = diff[VALUE] + diff[VALUE + '_deport']

    print('- Exchange Income')
    exchange = pd.concat([exchange, deportCompensation])
    
    if DEBUG:                
        print(exchange)

    exchange[SHARES] = exchange.apply(lambda x: x[SHARES] if x[TYPE] == PURCHASE else -x[SHARES], axis=1)
    exchangeIncome = data.computePortfolioIncome(exchange, c)
    # try:
    #     exchangeIncome = data.computePortfolioIncome(exchange, c)
    # except Exception as e:
    #     print(e)
    #     print(c)
    
    if DEBUG:
        print(exchangeIncome)

    exchangeIncome[CURRENCY] = exchangeIncome[SUBCATEGORY]

    if checkValues:
        print('- Gain')
        gain = pd.pivot_table(exchangeIncome, values=[VALUE], index=[SUBCATEGORY], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
        gain = gain.rename(columns={SUBCATEGORY: TICKER})
        
        calculatedBalance = pd.concat([aport, gain, deport]) 

        if DEBUG:
            prinT('gain')
            print(diff[diff['Diff'] != 0])
            print(gain[VALUE].sum())
            print(aport[VALUE].sum())
            print(deport[VALUE].sum())
            print(aport[VALUE].sum() + deport[VALUE].sum())
        
        return exchangeIncome, calculatedBalance

    return exchangeIncome


def calculateIncExp(k, calculatedBalance=None, flex=False, checkValues=True):
    prinT(k)
    income = data.readFlow('income', k, [CATEGORY, SUBCATEGORY], truncate=False, groupBy=None, filterRemove=True)
    expenses = data.readFlow('expenses', k, [CATEGORY, SUBCATEGORY], truncate=False, groupBy=None, filterRemove=True)

    if flex:
        income[DATE] = income[DATE].apply(lambda x: data.getFlexDate(x))
        expenses[DATE] = expenses[DATE].apply(lambda x: data.getFlexDate(x))

    income, portIncome = data.calculateBalance(income, c, k, [CATEGORY, SUBCATEGORY, DATE], PURCHASE)
    expenses, portExpenses = data.calculateBalance(expenses, c, k, [CATEGORY, SUBCATEGORY, DATE], '')
    
    if DEBUG:
        print(income)
        print(portIncome)
        print(portExpenses)
        print(expenses)

    income = pd.concat([income, portIncome, portExpenses])

    income[CURRENCY] = k
    expenses[CURRENCY] = k

    if checkValues:
        totalIncome = income[VALUE].sum() if not income.empty else 0
        totalExpenses = expenses[VALUE].sum() if not expenses.empty else 0
        calculatedBalance.loc[-1] = [k, totalIncome - totalExpenses]
        calculatedBalance = calculatedBalance.reset_index(drop=True)
        
        return income, expenses, portIncome, portExpenses, calculatedBalance

    return income, expenses, portIncome, portExpenses


def calcualteGlobalIncExp(calculatedBalance, currencies):
    print('- Income & Expenses')
    globalIncome = None
    globalExpenses = None
    for k in currencies:

        income, expenses, portIncome, portExpenses, calculatedBalance = calculateIncExp(k, calculatedBalance, checkValues=True)

        if globalIncome is None:
            globalIncome = income
            globalExpenses = expenses
        else:
            globalIncome = pd.concat([globalIncome, income])
            globalExpenses = pd.concat([globalExpenses, expenses])
    
    calculatedBalance = pd.pivot_table(calculatedBalance, values=[VALUE], index=[TICKER], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    
    return globalIncome, globalExpenses, calculatedBalance


def checkBalance(globalIncome, globalExpenses, exchangeIncome, realBalance):
    if DEBUG:
        prinT('realBalance')
        print(realBalance)
        print(realBalance[TOTAL].sum())
        print(realBalance[VALUE].sum())
    
        prinT('caculatedBalance')
        print('Income: ' + str(globalIncome[VALUE].sum()))
        print('Expenses: ' + str(globalExpenses[VALUE].sum()))
        print('Net: ' + str(globalIncome[VALUE].sum() - globalExpenses[VALUE].sum()))
        print('Gain: ' + str(exchangeIncome[VALUE].sum()))
        print('Total: ' + str(globalIncome[VALUE].sum() - globalExpenses[VALUE].sum() + exchangeIncome[VALUE].sum()))
        print(realBalance[VALUE].sum() - (globalIncome[VALUE].sum() - globalExpenses[VALUE].sum() + exchangeIncome[VALUE].sum()))

    globalIncome = pd.concat([globalIncome, exchangeIncome])
    # globalIncome[TICKER] = globalIncome[TICKER].apply(lambda x : data.mapTickers(x))
    # globalIncome[SUBCATEGORY] = globalIncome[SUBCATEGORY].apply(lambda x : data.mapTickers(x))
    # globalIncome[SUBCATEGORY] = globalIncome[SUBCATEGORY].apply(lambda x : data.mapTickers(x))
    globalIncome = pd.pivot_table(globalIncome, values=[VALUE], index=[DATE, CATEGORY, SUBCATEGORY], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    globalExpenses = pd.pivot_table(globalExpenses, values=[VALUE], index=[DATE, CATEGORY, SUBCATEGORY], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()

    globalIncome = globalIncome[globalIncome[VALUE] != 0]
    globalExpenses = globalExpenses[globalExpenses[VALUE] != 0]
    
    if DEBUG:
        print(globalIncome[VALUE].sum())
        print(globalExpenses[VALUE].sum())
        print(globalIncome[VALUE].sum() - globalExpenses[VALUE].sum())
    
    calculationError = round(realBalance[VALUE].sum() - (globalIncome[VALUE].sum() - globalExpenses[VALUE].sum()), 6)
    print('Calculation Error: ' + str(calculationError))

    globalIncome = globalIncome.rename(columns={VALUE: INCOME})
    globalExpenses = globalExpenses.rename(columns={VALUE: EXPENSES})

    return globalIncome, globalExpenses, calculationError


def calculateAllocation():
    positions = None
    # for c in ['USD']:
    for c in currencies:
        if positions is None: 
            positions = data.readAllocation(c, CURRENT_DATE)
        else:
            positions = pd.concat([positions, data.readAllocation(c, CURRENT_DATE)])
    return positions


def assetExchange(c, x):
    if x[TICKER] in set(['Fixed income (BRL)', 'Nubank', 'Savings']):
        return data.getExchange(c, 'BRL', getNextMonth(x[DATE]))
    else:
        return data.getExchange(c, x[TICKER], getNextMonth(x[DATE]))


def calculateFlexIncome(c, positions, portYield):
    # printVar('portYield', portYield)
    # positions['0_' + POSITION] = positions[POSITION]
    positions[POSITION] = positions.apply(lambda x: x[POSITION] * assetExchange(c, x), axis=1)
    positions[AVG_POSITION] = positions.apply(lambda x: x[AVG_POSITION] * assetExchange(c, x), axis=1)
    # positions[FLEX_POSITION] = positions.apply(lambda x: x[FLEX_POSITION] * assetExchange(c, x), axis=1)
    # flexIncome = pd.pivot_table(portYield, values=[VALUE], index=[SUBCATEGORY, DATE], aggfunc={VALUE: np.sum}, fill_value=0).reset_index()
    # flexIncome[TICKER] = flexIncome[SUBCATEGORY]
    # positions = positions.set_index([TICKER, DATE]).join(flexIncome.set_index([TICKER, DATE]), rsuffix='_inc')
    # positions = positions.reset_index()
    positions[SUBCATEGORY] = positions[TICKER]
    positions[CATEGORY] = positions[SUBCATEGORY].apply(lambda x: data.mapAllocationCategory(x))
    # if flexIncome.size > 0:
    #     positions[FLEX_INCOME] = positions[VALUE]
    positions = positions[[CATEGORY, SUBCATEGORY, DATE, POSITION, AVG_POSITION]]
    # positions = positions[[CATEGORY, SUBCATEGORY, DATE, POSITION, FLEX_POSITION, FLEX_INCOME]]
    print(positions)
    return positions


def uploadData(globalIncome, globalExpenses, positions, currency):
    print('Calculation: OK')
    globalIncome[SUBCATEGORY] = globalIncome.apply(lambda x : data.mapTickers(x[SUBCATEGORY]), axis=1)
    globalIncome[CATEGORY] = globalIncome.apply(lambda x : data.mapCategory(x[SUBCATEGORY], x[CATEGORY]), axis=1)
    records = globalIncome.to_dict('records') + globalExpenses.to_dict('records')
    balance = pd.concat([globalIncome, globalExpenses])
    balance[CURRENCY] = currency
    balance.to_csv('balance.csv')
    positions[SUBCATEGORY] = positions.apply(lambda x : data.mapTickers(x[SUBCATEGORY]), axis=1)
    positions[CATEGORY] = positions.apply(lambda x : data.mapCategory(x[SUBCATEGORY], x[CATEGORY]), axis=1)
    positions[CURRENCY] = currency
    positionsRecords = positions.to_dict('records')
    positions.to_csv('positions.csv')
    # print('Uploading data...')
    # mongo.replaceInsert(records, 'test')
    # mongo.replaceInsert(positionsRecords, 'positions')
    # print('DONE')


def saveData(globalIncome, globalExpenses, positions, currency):
    globalIncome[SUBCATEGORY] = globalIncome.apply(lambda x : data.mapTickers(x[SUBCATEGORY]), axis=1)
    globalIncome[CATEGORY] = globalIncome.apply(lambda x : data.mapCategory(x[SUBCATEGORY], x[CATEGORY]), axis=1)
    balance = pd.concat([globalIncome, globalExpenses])
    balance[CURRENCY] = currency
    positions[SUBCATEGORY] = positions.apply(lambda x : data.mapTickers(x[SUBCATEGORY]), axis=1)
    positions[CATEGORY] = positions.apply(lambda x : data.mapCategory(x[SUBCATEGORY], x[CATEGORY]), axis=1)
    positions[CURRENCY] = currency
    return balance, positions


if __name__ == '__main__':
    # currencies = ['BRL', 'IVVB11', 'AMZO34', 'BBSD11', 'BRAX11', 'DISB34', 'FBOK34', 'GOGL34', 'IRDM11', 'JPMC34', 'MSFT34', 'TSLA34', 'BTC', 'ETH', 'SOL', 'ADA', 'AVAX', 'USDT', 'AXS', 'SLP']
    if DEBUG:
        currencies = ['SLP']
        # currencies = ['BRL', DEBUG_CURRENCY]
    else:
        # currencies = ['ETH']
        currencies = get_currencies(CURRENT_DATE)


    validated = True
    print('Starting validation...')
    start_time = time()
    validated = validateRawData(currencies)
    print_if('Validation: OK', validated)
    print("--- %s seconds ---" % (time() - start_time))

    if validated:
        print('Calculating balance...')
        start_time = time()
        realBalance = calculateRawBalance(currencies)
        print("--- %s seconds ---" % (time() - start_time))
    
    bal = None
    pos = None
    if validated:
        # targetCurrencies = ['BRL']
        targetCurrencies = ['BRL', 'USD']
        for c in targetCurrencies:
            print(c)

            # print('Calculating allocation...')
            # start_time = time()
            # positions = calculateAllocation()
            # print("--- %s seconds ---" % (time() - start_time))

            # --- Overview --- #
            print('Calculating earnings...')
            start_time = time()
            exchangeIncome, calculatedBalance = calculateExchangeIncome(c)
            globalIncome, globalExpenses, calculatedBalance = calcualteGlobalIncExp(calculatedBalance, currencies)
            realBalance = calculateCurrencyBalance(c, calculatedBalance, realBalance)
            globalIncome, globalExpenses, calculationError = checkBalance(globalIncome, globalExpenses, exchangeIncome, realBalance)
            calculationError = 0
            print("--- %s seconds ---" % (time() - start_time))

            # --- Positions --- #
            print('Calculating positions...')
            start_time = time()
            # exchangeIncome, calculatedBalance = calculateExchangeIncome(c, flex=True)
            # portYield = None
            # # for k in ['USD']:
            # for k in currencies:
            #     income, expenses, portIncome, portExpenses = calculateIncExp(k, calculatedBalance=None, flex=True, checkValues=False)

            #     # print(income)
            #     # print(expenses)
            #     # print(portIncome)
            #     # print(portExpenses)
            #     if portYield is None:
            #         portYield = pd.concat([portIncome, portExpenses])
            #     else:
            #         portYield = pd.concat([portYield, portIncome, portExpenses])
            
            # portYield = pd.concat([portYield, exchangeIncome])
            positions = calculateAllocation()
            positions = calculateFlexIncome(c, positions, None)
            positions[POSITION] = positions[POSITION].apply(lambda x: round(x, 2))
            # positions[SUBCATEGORY] = positions.apply(lambda x : data.mapTickers(x[SUBCATEGORY]), axis=1)
            # positions[CATEGORY] = positions.apply(lambda x : data.mapCategory(x[SUBCATEGORY], x[CATEGORY]), axis=1)
            # positions[CURRENCY] = c
            # positions[FLEX_INCOME] = positions[FLEX_INCOME].fillna(0)
            print("--- %s seconds ---" % (time() - start_time))

            if calculationError == 0 and not DEBUG:
                print('Weeee')
                # uploadData(globalIncome, globalExpenses, positions, c)
                b, p = saveData(globalIncome, globalExpenses, positions, c)
                bal = b if bal is None else pd.concat([bal, b])
                pos = p if pos is None else pd.concat([pos, p])
    

    if validated:
        bal.to_csv('csv/dashboard/balance.csv')
        # positions.to_csv('csv/dashboard/positions.csv')
        pos.to_csv('csv/dashboard/positions.csv')
