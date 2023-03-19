create or replace view pyfinance.positions as 

with

    grid as (
        select distinct ticker, date_format(calendar_date, '%Y-%m-01') as calendar_month
        from pyfinance.prices
    ),

    cash_flow as (
        select 
            calendar_month,
            ticker,
            sum(units) as units,
            sum(avg_units) as avg_units
        from (
            select
                date_format(calendar_date, '%Y-%m-01') as calendar_month,
                currency as ticker,
                sum(amount) as units,
                sum(amount * (day(last_day(calendar_date)) - day(calendar_date) + 1)/day(last_day(calendar_date))) as avg_units
            from pyfinance.transactions
            where count_to_balance
            group by 1, 2
            union all
            select
                date_format(calendar_date, '%Y-%m-01') as calendar_month,
                ticker,
                sum(case when exchange_type = 'Purchase' then units else -units end) as units,
                sum(case when exchange_type = 'Purchase' then units * (day(last_day(calendar_date)) - day(calendar_date) + 1)/day(last_day(calendar_date)) 
                    else -units * (day(last_day(calendar_date)) - day(calendar_date) + 1)/day(last_day(calendar_date)) 
                end) as avg_units
            from pyfinance.exchanges
            where count_to_balance
            group by 1, 2
            union all
            select
                date_format(calendar_date, '%Y-%m-01') as calendar_month,
                currency as ticker,
                sum(case when exchange_type = 'Purchase' then -price*units else price*units end) as units,
                sum(case when exchange_type = 'Purchase' then price*units * (day(last_day(calendar_date)) - day(calendar_date) + 1)/day(last_day(calendar_date)) 
                    else -price*units * (day(last_day(calendar_date)) - day(calendar_date) + 1)/day(last_day(calendar_date)) 
                end) as avg_units
            from pyfinance.exchanges
            where count_to_balance
            group by 1, 2
        ) t
        group by 1, 2
    ),

    positions as (
        select 
            calendar_month,
            ticker,
            currency,
            units,
            avg_units,
            coalesce(lag(position) over (partition by ticker, currency order by calendar_month), 0) as prev_position,
            position,
            coalesce(lag(position) over (partition by ticker, currency order by calendar_month), 0) + avg_units as avg_position,
            coalesce(lag(price) over (partition by ticker, currency order by calendar_month), 0) as prev_price,
            price
        from (
            select
                g.calendar_month,
                g.ticker,
                ex.currency,
                round(coalesce(i.units, 0), 7) as units,
                round(coalesce(i.avg_units, 0), 7) as avg_units,
                round(sum(coalesce(i.units, 0)) over (partition by g.ticker, ex.currency order by g.calendar_month), 7) as position,
                round(ex.price, 7) as price
            from grid g
                left join cash_flow i on (g.calendar_month = i.calendar_month and g.ticker = i.ticker)
                left join pyfinance.exchange_rates ex on (g.calendar_month = ex.calendar_month and g.ticker = ex.ticker)
        ) t
    )

select *, position * price as c_position, avg_position * price as c_avg_position, (price-prev_price) * prev_position as c_value
from positions