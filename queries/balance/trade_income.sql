create or replace view pyfinance.trade_income as

with

    exchanges as (
        select
            e.calendar_date,
            e.ticker,
            e.currency,
            case when exchange_type = 'Purchase' then e.units else -e.units end as units,
            e.price as original_trade_price,
            ex.currency as ex_currency,
            ex.price as eom_price
        from pyfinance.exchanges e
            left join pyfinance.exchange_rates ex on (date_format(e.calendar_date, '%Y-%m-01') = ex.calendar_month and e.ticker = ex.ticker)
        where count_to_balance
        union all
        select
            e.calendar_date,
            e.currency as ticker,
            e.ticker as currency,
            case when exchange_type = 'Purchase' then -e.price*e.units else e.price*e.units end as units,
            1/e.price as original_trade_price,
            ex.currency as ex_currency,
            ex.price as eom_price
        from pyfinance.exchanges e
            left join pyfinance.exchange_rates ex on (date_format(e.calendar_date, '%Y-%m-01') = ex.calendar_month and e.currency = ex.ticker)
        where count_to_balance
    ),

    direct_calculation as (
        select
            date_format(calendar_date, '%Y-%m-01') as calendar_month,
            ticker,
            currency,
            units,
            original_trade_price,
            ex_currency,
            eom_price,
            (eom_price - original_trade_price) * units as c_value
        from exchanges
        where currency = ex_currency
    ),

    price_estimation as (
        select
            date_format(e.calendar_date, '%Y-%m-01') as calendar_month,
            e.ticker,
            e.currency,
            e.units,
            e.original_trade_price,
            e.ex_currency,
            day(e.calendar_date) as days_passed,
            ex1.price as som_price,
            e.eom_price,
            day(e.calendar_date)/30 * (e.eom_price-ex1.price) + ex1.price as estimate_1,
            ex2.price as c_som_price,
            ex3.price as c_eom_price,
            (day(e.calendar_date)/30 * (ex3.price-ex2.price) + ex2.price) as c_price,
            (day(e.calendar_date)/30 * (ex3.price-ex2.price) + ex2.price) * e.original_trade_price as estimate_2,
            (e.original_trade_price / (eom_price / ex3.price)) * e.eom_price as estimate_3
        from exchanges e
            left join pyfinance.exchange_rates ex1 on (
                date_add(date_format(e.calendar_date, '%Y-%m-01'), interval -1 month) = ex1.calendar_month 
                and e.ticker = ex1.ticker
                and e.ex_currency = ex1.currency
                )
            left join pyfinance.exchange_rates ex2 on (
                date_add(date_format(e.calendar_date, '%Y-%m-01'), interval -1 month) = ex2.calendar_month 
                and e.currency = ex2.ticker
                and e.ex_currency = ex2.currency
                )
            left join pyfinance.exchange_rates ex3 on (
                date_format(e.calendar_date, '%Y-%m-01') = ex3.calendar_month 
                and e.currency = ex3.ticker
                and e.ex_currency = ex3.currency
                )
        where e.currency != e.ex_currency
            and (e.ticker != e.ex_currency
                and eom_price != 1
                and original_trade_price != 1 
            )    
    ),

    indirect_calculation as (
        select *,
            case when days_passed < 5 or days_passed > 25 then (
                0.8*coalesce(estimate_1, estimate_3, estimate_2) 
                + 0.15*coalesce(estimate_2, estimate_3, estimate_1) 
                + 0.05*coalesce(estimate_3, estimate_2, estimate_1)
            ) else (
                0.1*coalesce(estimate_1, estimate_3, estimate_2) 
                + 0.2*coalesce(estimate_2, estimate_3, estimate_1) 
                + 0.7*coalesce(estimate_3, estimate_2, estimate_1)
            ) end as final_estimate,
            case when days_passed < 5 or days_passed > 25 then (eom_price -
                (0.8*coalesce(estimate_1, estimate_3, estimate_2) 
                + 0.15*coalesce(estimate_2, estimate_3, estimate_1) 
                + 0.05*coalesce(estimate_3, estimate_2, estimate_1))
            ) else (eom_price - (
                0.1*coalesce(estimate_1, estimate_3, estimate_2) 
                + 0.2*coalesce(estimate_2, estimate_3, estimate_1) 
                + 0.7*coalesce(estimate_3, estimate_2, estimate_1)
            )) end * units as c_value
        from price_estimation
    )


select
    calendar_month,
    ticker,
    ex_currency as currency,
    sum(c_value) as income
from (
    select 
        calendar_month,
        ticker,
        ex_currency,
        c_value
    from direct_calculation
    union all
    select 
        calendar_month,
        ticker,
        ex_currency,
        c_value
    from indirect_calculation
) t
group by 1, 2, 3