import pandas as pd
import datetime as dt
from time import time

from lib import tools, mysql_lib
from lib.constants import *


def reset_table(b):
    if 'drop' in db[b]:
        df = mysql_lib.execute_query(db[b]['drop'], mode='management')
        df = mysql_lib.execute_query(db[b]['create'], mode='management')
    df = pd.read_csv(csv[b])
    if b == 'prices':
        df = pd.melt(df, id_vars=[TICKER, CURRENCY], var_name=DATE, value_name=PRICE)
        df = df.dropna()
    # if DATE in df:
    if DATE in df and b not in ['transactions', 'transfers']:
        df[DATE] = df[DATE].apply(lambda x : dt.datetime.strptime(str(x),'%d/%m/%Y'))#.date())
    if b in ['income']:
        df[TTYPE] = INCOME
    if b in ['expenses', 'adjustments']:
        df[AMOUNT] = -df[AMOUNT]
        df[TTYPE] = EXPENSES
    if b == 'income_mapping':
        df = df.drop_duplicates()
    mysql_lib.insert_df(df, db[b]['table'], db[b]['columns'], merge=(len(db[b]['columns']) > 0))


def db_reset_block(bases=[]):
    print('\n---------------- Reset block -----------------')
    start_time = time()
    for b in bases:
        print(b)
        try:
            reset_table(b)
        except Exception as e:
            print('=====', b, '=====')
            print(e)
            return -1
    print('Success')
    print("--------------- %.4f seconds ---------------" % (time() - start_time))
    return (time() - start_time)


def validation_block():
    print('\n-------------- Validation block --------------')
    start_time = time()
    df = mysql_lib.execute_query('queries/validation/validation.sql')
    validated = df.size == 0
    if not validated:
        print('====ERROR====')
        print(df)
    tools.print_if('Validation: OK', validated)
    print("--------------- %.4f seconds ---------------" % (time() - start_time))
    return validated, (time() - start_time)


def adjust_minor_differences(df_adjust):
    df_exp = pd.read_csv('csv/data/data_expenses.csv')
    df_exp = pd.concat([df_exp, df_adjust])
    df_exp = df_exp.reset_index(drop=True)
    df_exp.to_csv('csv/data/data_expenses_adjusted.csv', index=False)


def adjustment_block():
    print('\n============== Adjustment block ==============')
    start_time = time()
    df_adjust = mysql_lib.execute_query('queries/validation/adjustments.sql')
    df_adjust[DATE] = df_adjust[DATE].apply(lambda x : x.strftime('%d/%m/%Y'))#.date())
    flag = mysql_lib.execute_query('queries/validation/adjustment_flag.sql')
    print(df_adjust)
    if flag.iloc[0, 0] == 1:
        adjust_minor_differences(df_adjust)
        db_reset_block(['income', 'adjustments'])
        validated = validation_block()
        if not validated:
            db_reset_block(['income', 'expenses'])
            print('Adjustment failed...\nRolled back')
            return (time() - start_time)
        else:
            df_exp = pd.read_csv('csv/data/data_expenses_adjusted.csv')
            df_exp.to_csv('csv/data/data_expenses.csv', index=False)
            print('Adjustments commited!')
            print("=============== %.4f seconds ===============" % (time() - start_time))
            return (time() - start_time)
    else:
        print('Adjustment failed...\nMajor discrepancies')
        raise RuntimeError
        return (time() - start_time)


def dash_block():
    print('\n-------------- Balance block ----------------')
    start_time = time()
    df_balance = mysql_lib.execute_query('queries/dash/balance.sql')
    df_balance.to_csv('csv/dashboard/balance.csv', index=False)
    df_balance = mysql_lib.execute_query('queries/dash/allocation.sql')
    df_balance.to_csv('csv/dashboard/positions.csv', index=False)
    print('Success')
    print("--------------- %.4f seconds ---------------" % (time() - start_time))
    return (time() - start_time)


if __name__ == '__main__':
    run_time = 0
    run_time = db_reset_block(['income', 'expenses', 'exchange', 'transf', 'balances', 'prices'])
    validated, run_time_v = validation_block()
    if not validated:
        run_time += adjustment_block()
    run_time += dash_block()        
    print("\nTotal run time: %.4f seconds" % (run_time + run_time_v))


    