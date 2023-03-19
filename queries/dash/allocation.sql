select
    calendar_month,
    cm.category,
    cm.subcategory,
    p.currency,
    sum(c_position) as position,
    sum(c_avg_position) as avg_position
from pyfinance.positions p 
    left join pyfinance.full_category_mapping cm on (p.ticker = cm.currency)
where c_position != 0
group by 1, 2, 3, 4