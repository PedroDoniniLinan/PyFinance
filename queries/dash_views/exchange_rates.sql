create or replace view pyfinance.exchange_rates as

with

    grid as (
        select distinct
            ticker,
            'USD' as currency,
            calendar_date
        from pyfinance.prices
        union all
        select distinct
            ticker,
            'BRL' as currency,
            calendar_date
        from pyfinance.prices
    ),

    extended_conversion as (
        select distinct
            p1.ticker as ticker,
            p1.currency as c1,
            p2.currency as c2, 
            p3.ticker as c3,
            p1.calendar_date,
            p1.price as direct_conversion,
            p1.price * p2.price as indirect_conversion,
            p1.price / p3.price as indirect_reverse_conversion
        from pyfinance.prices p1
            left join pyfinance.prices p2 on (p1.currency = p2.ticker and p1.calendar_date = p2.calendar_date)
            left join pyfinance.prices p3 on (p1.currency = p3.currency and p1.calendar_date = p3.calendar_date)
        where (p1.currency = 'USD' or p2.currency = 'USD' or p3.ticker = 'USD')
            or (p1.currency = 'BRL' or p2.currency = 'BRL' or p3.ticker = 'BRL')
    ),

    consolidation as (
        select distinct
            g.ticker,
            g.currency,
            date_format(g.calendar_date, '%Y-%m-01') as calendar_month,
            case when e1.c1 = g.currency then e1.direct_conversion
                when e1.c2 = g.currency then e1.indirect_conversion
                when e1.c3 = g.currency then e1.indirect_reverse_conversion
            end as price,
            case when e1.c1 = g.currency then 1
                when e1.c2 = g.currency then 2
                when e1.c3 = g.currency then 3
            end as priority
        from grid g
            left join extended_conversion e1 on (
                g.ticker = e1.ticker 
                and g.calendar_date = e1.calendar_date 
                and (e1.c1 = g.currency or e1.c2 = g.currency or e1.c3 = g.currency)
            )
        order by 1, 2, 3
    )

select 
    ticker,
    currency,
    calendar_month,
    price
from (    
    select *, row_number() over (partition by ticker, currency, calendar_month order by priority) as rn
    from consolidation
) t
where rn = 1
