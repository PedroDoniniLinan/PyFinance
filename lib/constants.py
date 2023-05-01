BALANCE = 'balance'
DATE = 'calendar_date'
AMOUNT = 'amount'
TICKER = 'ticker'
CURRENCY = 'currency'
PRICE = 'price'
TTYPE = 'transaction_type'

NAME = 'Name'
VALUE = 'Value'
FLEX_VALUE = 'Flex value'
ACCOUNT = 'account'
CATEGORY = 'Category'
SUBCATEGORY = 'Subcategory'
CURRENCY = 'Currency'
ORIGINAL_CURRENCY = 'Original currency'
POSITION = 'Position'
AVG_POSITION = 'Avg position'
FLEX_POSITION = 'Flex position'
FLEX_INCOME = 'Flex income'
REMOVE = 'Remove'
APORT = 'Aport'
PRICE = 'Price'
TOTAL = 'Total'

NU = 'Nu'
EASY = 'Easy'
BT = 'BT'
BNB = 'BNB'
BFI = 'BlockFi'
INVESTMENTS = 'Stocks'

GROWTH_STOCKS = 'Growth stocks'
BONDS = 'Bonds'
SAVINGS = 'Savings'
EMERGENCY = 'Emergency fund'
FIXED_INCOME = 'Fixed income'
CASH = 'Cash'
REAL_STATE = 'Real state'
CRYPTO = 'Crypto'
STABLECOIN = 'Stablecoin'
COMMOD = 'Commodities'
VALUE_STOCKS = 'Value stocks'
US_VALUE = 'US value'
US_GROWTH = 'US growth'
US_FINANCES = 'US finances'
US_SUPER_GROWTH = 'US super growth'
BR_GROWTH = 'BR growth'
AXIE = 'Axie'
STAR = 'Star atlas'
THETAN = 'Thetan'
DEFINA = 'Defina'
NFTG = 'NFT games'

INCOME = 'Income'
EXPENSES = 'Expenses'

FROM = 'From'
TO = 'To'

TICKER = 'Ticker'
BUY = 'Buy'
SHARES = 'Shares'
BUY_TAX = 'Buy Tax'
ENTER_DATE = 'Enter date'
TYPE = 'Type'
PURCHASE = 'Purchase'
SALE = 'Sale'
BUY_TAX_CURRENCY = 'Buy Tax Currency'

IN = 'In'
OUT = 'Out'
EXCHANGE = 'Exchange'

DATE_TRESHOLD = 20

COLS = [INCOME, EXPENSES, OUT, IN, EXCHANGE]

# DB Update

csv = {
    'balances': 'csv/data/data_balances.csv',
    'income': 'csv/data/data_income.csv',
    'adjustments': 'csv/data/data_expenses_adjusted.csv',
    'expenses': 'csv/data/data_expenses.csv',
    'exchange': 'csv/data/data_exchange.csv',
    'transf': 'csv/data/data_transf.csv',
    'prices': 'csv/data/data_prices.csv',
    'income_mapping': 'csv/tables/income_mapping.csv',
    'transfers': 'csv/tables/transfers.csv',
    'transactions': 'csv/tables/transactions.csv'
}

db = {
    'balances': {'table': 'pyfinance.balances', 'columns':['amount'], 'drop': 'queries/table_creation/drop_balances.sql', 'create': 'queries/table_creation/balances.sql'},
    'income': {'table': 'pyfinance.transactions', 'columns':[], 'drop': 'queries/table_creation/drop_transactions.sql', 'create': 'queries/table_creation/transactions.sql'},
    'adjustments': {'table': 'pyfinance.transactions', 'columns':[]},
    'expenses': {'table': 'pyfinance.transactions', 'columns':[]},
    'exchange': {'table': 'pyfinance.exchanges', 'columns':[], 'drop': 'queries/table_creation/drop_exchanges.sql', 'create': 'queries/table_creation/exchanges.sql'},
    'transf': {'table': 'pyfinance.transfers', 'columns':[], 'drop': 'queries/table_creation/drop_transfers.sql', 'create': 'queries/table_creation/transfers.sql'},
    'prices': {'table': 'pyfinance.prices', 'columns':[], 'drop': 'queries/table_creation/drop_prices.sql', 'create': 'queries/table_creation/prices.sql'},
    'income_mapping': {'table': 'pyfinance.category_mapping', 'columns':[], 'drop': 'queries/table_creation/drop_mapping.sql', 'create': 'queries/table_creation/mapping.sql'},
    'transfers': {'table': 'pyfinance.transfers', 'columns':['source_acc', 'destination_acc', 'calendar_date', 'amount', 'currency', 'count_to_balance', 'transaction_type']},
    'transactions': {'table': 'pyfinance.transactions', 'columns':['tag', 'amount', 'account', 'calendar_date', 'category', 'subcategory', 'count_to_balance', 'currency']},
    'irpf': {'table': 'pyfinance.irpf', 'columns':[], 'drop': 'queries/table_creation/drop_irpf.sql', 'create': 'queries/table_creation/irpf.sql'},
    'sales': {'table': 'pyfinance.sales', 'columns':[], 'drop': 'queries/table_creation/drop_sales.sql', 'create': 'queries/table_creation/sales.sql'},
}
