with 

    balances as (
        select
            account,
            currency,
            calendar_date,
            amount as balance
        from pyfinance.balances
        where calendar_date in (
                select max(calendar_date) as calendar_date
                from pyfinance.balances
            )
    ),

    cash_in as (
        select
            account,
            currency,
            sum(amount) as delta
        from pyfinance.transactions
        where amount >= 0
        group by 1, 2
    ),

    cash_out as (
        select
            account,
            currency,
            sum(amount) as delta
        from pyfinance.transactions
        where amount < 0
        group by 1, 2
    ),

    aport as (
        select 
            account,
            currency,
            sum(delta) as delta
        from (
            select
                account,
                ticker as currency,
                sum(units) as delta
            from pyfinance.exchanges
            where exchange_type = 'Purchase'
            group by 1, 2
            union all
            select
                account,
                currency,
                sum(price * units) as delta
            from pyfinance.exchanges
            where exchange_type = 'Sale'
            group by 1, 2
        ) t
        group by 1, 2
    ),

    deport as (
        select 
            account,
            currency,
            sum(delta) as delta
        from (
            select
                account,
                ticker as currency,
                sum(-units) as delta
            from pyfinance.exchanges
            where exchange_type = 'Sale'
            group by 1, 2
            union all
            select
                account,
                currency,
                sum(-price * units) as delta
            from pyfinance.exchanges
            where exchange_type = 'Purchase'
            group by 1, 2
        ) t
        group by 1, 2
    ),

    transf_in as (
        select
            destination_acc as account,
            currency,
            sum(amount) as delta
        from pyfinance.transfers
        group by 1, 2
    ),

    transf_out as (
        select
            source_acc as account,
            currency,
            sum(-amount) as delta
        from pyfinance.transfers
        group by 1, 2
    ),

    consolidation as (
        select
            account,
            currency,
            calendar_date,
            balance,
            cast(round(income + expenses + aport + deport + transf_in + transf_out, 7) as char) as calculated_balance,
            cast(round(balance - (income + expenses + aport + deport + transf_in + transf_out), 7) as char) as difference,
            income,
            expenses,
            aport,
            deport,
            transf_in,
            transf_out
        from (
            select
                b.*,
                coalesce(ci.delta, 0) as income,
                coalesce(co.delta, 0) as expenses,
                coalesce(a.delta, 0) as aport,
                coalesce(d.delta, 0) as deport,
                coalesce(ti.delta, 0) as transf_in,
                coalesce(t.delta, 0) as transf_out
            from balances b
                left join cash_in ci on (ci.account = b.account and ci.currency = b.currency)
                left join cash_out co on (co.account = b.account and co.currency = b.currency)
                left join aport a on (a.account = b.account and a.currency = b.currency)
                left join deport d on (d.account = b.account and d.currency = b.currency)
                left join transf_in ti on (ti.account = b.account and ti.currency = b.currency)
                left join transf_out t on (t.account = b.account and t.currency = b.currency)
        ) t
        order by 1, 2
    )

select *
from consolidation
where round(difference, 7) != 0
order by abs(difference) desc