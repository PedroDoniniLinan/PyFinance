create or replace view pyfinance.trade_income as

with

    exchanges as (
        select
            e.calendar_date,
            date_format(e.calendar_date, '%Y-%m-01') as calendar_month,
            e.ticker,
            e.currency,
            case when exchange_type = 'Purchase' then e.units else -e.units end as units,
            e.price as original_trade_price,
            exe.currency as ex_currency,
            day(e.calendar_date) as days_passed,
            exs.price as som_price,
            exe.price as eom_price,
            day(e.calendar_date)/day(last_day(e.calendar_date)) * (exe.price-exs.price) + exs.price as linear_estimate,
            excs.price as c_som_price,
            exce.price as c_eom_price,
            (day(e.calendar_date)/day(last_day(e.calendar_date)) * (exce.price-excs.price) + excs.price) as c_price,
            (day(e.calendar_date)/day(last_day(e.calendar_date)) * (exce.price-excs.price) + excs.price) * e.price as c_linear_estimate,
            (e.price / (exe.price / exce.price)) * exe.price as proportional_estimate,
            (0.1 + 1.8*(0.5*day(last_day(e.calendar_date))-day(e.calendar_date))/day(last_day(e.calendar_date))) as linear_alfa
        from pyfinance.exchanges e
            left join pyfinance.exchange_rates exe on (date_format(e.calendar_date, '%Y-%m-01') = exe.calendar_month and e.ticker = exe.ticker)
            left join pyfinance.exchange_rates exs on (
                date_add(date_format(e.calendar_date, '%Y-%m-01'), interval -1 month) = exs.calendar_month 
                and e.ticker = exs.ticker
                and exe.currency = exs.currency
                )
            left join pyfinance.exchange_rates excs on (
                date_add(date_format(e.calendar_date, '%Y-%m-01'), interval -1 month) = excs.calendar_month 
                and e.currency = excs.ticker
                and exe.currency = excs.currency
                )
            left join pyfinance.exchange_rates exce on (
                date_format(e.calendar_date, '%Y-%m-01') = exce.calendar_month 
                and e.currency = exce.ticker
                and exe.currency = exce.currency
                )
        where count_to_balance
    ),

    price_estimation as (
        select
            calendar_month,
            calendar_date,
            ticker,
            currency,
            units,
            original_trade_price,
            ex_currency,
            days_passed,
            som_price,
            eom_price,
            c_som_price,
            c_eom_price,
            c_price,
            linear_estimate,
            c_linear_estimate,
            proportional_estimate,
            case when currency = ex_currency then (
                original_trade_price 
            ) when currency in ('EUR', 'USD', 'BRL', 'USDC', 'BUSD', 'USDT', 'GUSD') and calendar_month < '2020-01-01' then (
                coalesce(c_linear_estimate, proportional_estimate, linear_estimate)
            ) when ticker in ('EUR', 'USD', 'BRL', 'USDC', 'BUSD', 'USDT', 'GUSD') and calendar_month < '2020-01-01' then (
                coalesce(linear_estimate, proportional_estimate, c_linear_estimate)
            ) else (
                linear_alfa * coalesce(linear_estimate, proportional_estimate, c_linear_estimate) 
                + (1-linear_alfa) * coalesce(c_linear_estimate, proportional_estimate, linear_estimate) 
            ) end as final_estimate                
        from exchanges         
    ),

    trade_income as (
        select
            calendar_month,
            calendar_date,
            ticker,
            currency,
            units,
            original_trade_price,
            ex_currency,
            (eom_price - final_estimate) * units as trade_income,
            (-c_eom_price * original_trade_price + final_estimate) * units as c_trade_income,
            (eom_price - final_estimate) * units + (-c_eom_price * original_trade_price + final_estimate) * units as total_trade_income,
            (eom_price - c_eom_price * original_trade_price) * units as direct_trade_income,
            final_estimate,
            days_passed,
            som_price,
            eom_price,
            c_som_price,
            c_eom_price,
            c_price,
            linear_estimate,
            c_linear_estimate,
            proportional_estimate
        from price_estimation
    )

select *
from trade_income