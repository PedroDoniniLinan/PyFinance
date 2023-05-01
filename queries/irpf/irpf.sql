with

    arrangement as (
        select
            id,
            ticker,
            calendar_date,
            date_format(calendar_date, '%Y-01-01') as calendar_year,
            avg_price,
            units,
            position,
            row_number() over (partition by ticker, date_format(calendar_date, '%Y-01-01') order by calendar_date desc, id desc) as rn
        from pyfinance.irpf
    )

select
    ticker,
    calendar_date,
    calendar_year,
    avg_price,
    units,
    position
from arrangement
where rn = 1
order by 1, 2