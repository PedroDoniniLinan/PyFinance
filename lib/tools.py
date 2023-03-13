from lib.constants import *

import datetime as dt
import pandas as pd


def date_trunc(date, unit=None):
    if unit == 'year':
        return date.apply(lambda x : dt.datetime(x.year, 1, 1))#.date())
    else:
        return date.apply(lambda x : dt.datetime(x.year, x.month, 1))#.date())


def getNextDay(date):
    if date.month == 12 and date.day == 31:
        return dt.datetime(date.year + 1, 1, 1)
    elif date.day == 31 and date.month in (1, 3, 5, 7, 8, 10) or date.day == 30 and date.month in (4, 6, 9, 11) or date.day == 28 and date.month == 2:
        return dt.datetime(date.year, date.month + 1, 1)
    else:
        return dt.datetime(date.year, date.month, date.day + 1)


def getNextMonth(date, unit=None):
    if date.month == 12:
        return dt.datetime(date.year + 1, 1, 1)
    else:
        return dt.datetime(date.year, date.month + 1, 1)


def getPrevMonth(date, unit=None):
    if date.month == 1:
        return dt.datetime(date.year - 1, 12, 1)
    else:
        return dt.datetime(date.year, date.month - 1, 1)


def read(path, currency, filterRemove):
    df = pd.read_csv(path)
    if not(currency is None):
        df = df[df[CURRENCY] == currency]
    if filterRemove:
        df = df[df[REMOVE] == False]
    df.pop(REMOVE)
    return df


def getActives(date):
    df = pd.read_csv('csv/data/data_balances.csv')
    df = df[df[DATE] == date]
    return sorted(list(set(list(df[CURRENCY].unique()))))
    # df = pd.read_csv('csv/data/data_exchange.csv')
    # return sorted(list(set(list(df[TICKER].unique())) | set(list(df[CURRENCY].unique()))))


def prinT(text):
    print('--------------- ' + text + ' ---------------')


def printVar(text, var):
    print()
    print('------------------------------------------------')
    print('//' + text)
    print('------------------------------------------------')
    print(var)

def printIf(msg, condition):
    if condition:
        print(msg)