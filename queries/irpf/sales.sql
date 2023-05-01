with

    securities as (
        select *,
            case when security_type in ('ETF', 'Stocks', 'BDR') then 'Market'
                when security_type in ('FI') then 'FI'
                else security_type
            end compensation_group,
            date_format(calendar_date, '%Y-%m-01') as calendar_month
        from pyfinance.sales
    ),

    sale_results as (
        select *,
            sum(abs(sale_amount)) over (partition by security_type, calendar_month) as volume_sold,
            sum(profit) over (partition by security_type, calendar_month) as month_profit,
            sum(profit) over (partition by compensation_group) as total_profit
        from securities
    )

select *,
    case when security_type in ('Stocks', 'Crypto/NFT') then volume_sold > 30000
        when security_type in ('FI') then volume_sold > 15000
        else 1
    end volume_flag,
    month_profit > 0 as profit_flag,
    total_profit > 0 as total_profit_flag
from sale_results
order by security_type, calendar_date, ticker