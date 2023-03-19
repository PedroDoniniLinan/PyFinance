create or replace view pyfinance.full_category_mapping as

with

    bonds as (
        select distinct ticker
        from pyfinance.prices
        where lower(ticker) like 'cdb%' or lower(ticker) like 'td%' 
    ),

    classification as (
        select
            ticker,
            case when lower(ticker) like 'cdb%' then 'CDB' else 'TD' end as sub_1,
            case when lower(ticker) like '%cdi%' then 'CDI' 
                when lower(ticker) like '%prefix%' then 'Prefix' 
                when lower(ticker) like '%ipca%' then 'IPCA' 
                else 'SELIC' 
            end as sub_2,
            case when lower(ticker) like '%b' then 'B' 
                when lower(ticker) like '%l' then 'L' 
                else 'E' 
            end as sub_3
        from bonds
    )

select distinct
    case when sub_3 = 'B' then 'Bonds'
        when sub_3 = 'L' then 'Savings'
        else 'Emergency fund'
    end as category,
    concat(sub_1, ' ', sub_2, ' ', sub_3) as subcategory,
    ticker as currency,
    'Income' as category_type
from classification
union all
select * from pyfinance.category_mapping