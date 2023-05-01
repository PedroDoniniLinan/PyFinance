with

    balances as (
        select
            currency as cc,
            sum(amount) as balance
        from pyfinance.balances
        where calendar_date in (
                select distinct max(calendar_date) as calendar_date
                from pyfinance.balances
                where calendar_date <= '2023-01-04'
            )
        group by 1
    ),

    flow as (
        select
            -- date_add(date_format(calendar_date, '%Y-01-01'), interval 1 year) as calendar_year,
            -- date_add(calendar_date, interval -1 month) as calendar_date,
            calendar_date,
            currency,
            -- max(date_add(calendar_date, interval 1 month)) as calendar_date,
            sum(amount) as delta
        from pyfinance.transactions
        where calendar_date <= '2022-12-31'
        group by 1, 2
    ),

    acquisition_value as (
        select
            id,
			'direct' as tag,
            calendar_date,
            ticker,
            currency,
            original_trade_price,
            units,
            tax,
            tax_currency,
            round(final_estimate, 4) as final_estimate,
            case when units < 0 then 0
                when currency = 'BRL' and tax_currency = 'BRL' then original_trade_price * units + tax
                when currency != 'BRL' and tax_currency = 'BRL' then round(final_estimate, 4) * units + tax
                when tax_currency = 'BNB' and currency = 'BRL' then original_trade_price * units * 1.0075
                when tax_currency = 'BNB' and currency != 'BRL' then round(final_estimate, 4) * units * 1.0075
                when currency = 'BRL' then original_trade_price * units
                else round(final_estimate, 4) * units
            end as acquired_value,
            case when ticker = tax_currency then (units - tax)
                else units
            end as acquired_units
        from pyfinance.trade_income
        where ex_currency = 'BRL'
            and calendar_date <= '2022-12-31'
        union all
        select
            id,
			'reverse' as tag,
            calendar_date,
            currency as ticker,
            ticker as currency,
            1 / original_trade_price as original_trade_price,
            -original_trade_price * units as units,
            tax,
            tax_currency,
            round(final_estimate, 4)/original_trade_price as final_estimate,
            case when -original_trade_price * units < 0 then 0 
                when currency = 'BRL' and tax_currency = 'BRL' then 1 / original_trade_price * (-original_trade_price * units) + tax
                when currency != 'BRL' and tax_currency = 'BRL' then round(final_estimate, 4)/original_trade_price * (-original_trade_price * units) + tax
                when tax_currency = 'BNB' and currency = 'BRL' then 1 / original_trade_price * (-original_trade_price * units) * 1.0075
                when tax_currency = 'BNB' and currency != 'BRL' then round(final_estimate, 4)/original_trade_price * (-original_trade_price * units) * 1.0075
                when currency = 'BRL' then 1 / original_trade_price * (-original_trade_price * units)
                else round(final_estimate, 4)/original_trade_price * (-original_trade_price * units)
            end as acquired_value,
            case when ticker = tax_currency then (-original_trade_price * units - tax)
                else -original_trade_price * units
            end as acquired_units
        from pyfinance.trade_income
        where ex_currency = 'BRL'
            and calendar_date <= '2022-12-31'
            and currency != 'BRL'
        union all
        select
            10000 as id,
            'flow' as tag,
            calendar_date,
            currency as ticker,
            'BRL' as currency,
            0 as original_trade_price,
            delta as units,
            0 as tax,
            'BRL' as tax_currency,
            0 as final_estimate,
            0 as acquired_value,
            delta as acquired_units
        from flow
    ),

    consolidation as (
        select
            id,
            tag,
            calendar_date,
            case when ticker in ('ADA', 'AXS', 'AVAX', 'BTC', 'BUSD', 'DOT', 'ETH', 'MATIC', 'SOL', 'USDC', 'USDT') then 'Crypto/NFT'
                when ticker in ('BBSD11', 'BRAX11', 'IVVB11') then 'ETF'
                when ticker in ('DISB34', 'FBOK34', 'GOGL34', 'JPMC34', 'MSBR34', 'TSLA34') then 'BDR'
                when ticker in ('KO', 'MS', 'O', 'SPY', 'TSLA', 'V', 'VNQ') then 'Stocks'
                when ticker in ('USD') then 'Forex'
                when ticker in ('IRDM11', 'RBHY') then 'FI'
            end as security_type,
            ticker,
            currency,
            original_trade_price,
            units,
            tax,
            tax_currency,
            final_estimate,
            round(acquired_value, 7) as acquired_value,
            round(acquired_units, 7) as acquired_units,
            round(sum(acquired_units) over (partition by ticker order by calendar_date, id), 7) as total_units,
            b.balance
        from acquisition_value a
            left join balances b on (a.ticker = b.cc)
        where ticker not in ('BRL', 'Nubank', 'Rivalry', 'LTC', 'BNB', 
            'EUR', 'MTD', 'MTF', 'LUNA', 'LUNA2', 'GUSD', 'PHPD', 'POLIS', 
            'RUS', 'UST', 'SXLU', 'SLP', 'ATLAS', 'BCOIN', 'FINA', 'RON', 
            'SAND', 'SHIB', 'THC', 'THG')
            and currency not in ('Nubank', 'Rivalry')
    )

select *, round(100 * abs(balance - total_units) / balance, 2) as diff, CONCAT(round(100 * abs(balance - total_units) / balance, 2), ' %') as pct_diff
from consolidation
order by ticker, calendar_date, id