with

    expenses as (
        select
            date_format(calendar_date, '%Y-%m-01') as calendar_month,
            category,
            subcategory,
            currency,
            sum(amount) as expenses
        from pyfinance.transactions
        where transaction_type = 'Expenses'
            and count_to_balance
        group by 1, 2, 3, 4
    ),

    converted_expenses as (
        select
            e.calendar_month,
            e.category,
            e.subcategory,
            ex.currency,
            sum(e.expenses * ex.price) as expenses
        from expenses e
            left join pyfinance.exchange_rates ex on (e.calendar_month = ex.calendar_month and e.currency = ex.ticker)
        group by 1, 2, 3, 4
    ),

    income as (
        select
            date_format(calendar_date, '%Y-%m-01') as calendar_month,
            category,
            subcategory,
            currency,
            sum(amount) as income
        from pyfinance.transactions
        where transaction_type = 'Income'
            and count_to_balance
        group by 1, 2, 3, 4
    ),

    converted_income as (
        select
            i.calendar_month,
            case when i.subcategory = 'Rivalry' or cm.category is null then i.category
                else cm.category
            end as category,
            coalesce(cm.subcategory, i.subcategory) as subcategory,
            ex.currency,
            sum(i.income * ex.price) as income
        from income i
            left join pyfinance.exchange_rates ex on (i.calendar_month = ex.calendar_month and i.currency = ex.ticker)
            left join pyfinance.full_category_mapping cm on (i.subcategory = cm.currency and cm.category_type = 'Income')
        group by 1, 2, 3, 4
    ),

    valuation_income as (
        select
            calendar_month,
            cm.category,
            cm.subcategory,
            p.currency,
            sum(c_value) as income
        from pyfinance.positions p 
            left join pyfinance.full_category_mapping cm on (p.ticker = cm.currency and cm.category_type = 'Income')
        where c_value != 0
        group by 1, 2, 3, 4
    ),

    trade_income as (
        select
            calendar_month,
            cm.category,
            cm.subcategory,
            p.currency,
            sum(income) as income
        from pyfinance.trade_income p 
            left join pyfinance.full_category_mapping cm on (p.ticker = cm.currency and cm.category_type = 'Income')
        where income != 0
        group by 1, 2, 3, 4
    )


select
    calendar_month,
    category,
    subcategory,
    currency,
    sum(income) as income,
    sum(expenses) as expenses 
from (
    select
        calendar_month,
        category,
        subcategory,
        currency,
        income,
        0 as expenses
    from converted_income
    union all
    select
        calendar_month,
        category,
        subcategory,
        currency,
        income,
        0 as expenses
    from valuation_income
    union all
    select
        calendar_month,
        category,
        subcategory,
        currency,
        income,
        0 as expenses
    from trade_income
    union all
    select
        calendar_month,
        category,
        subcategory,
        currency,
        0 as income,
        expenses
    from converted_expenses
) t
where income is not null
    or expenses is not null
group by 1, 2, 3, 4
order by 1, 2, 3