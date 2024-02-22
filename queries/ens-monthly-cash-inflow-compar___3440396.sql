-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: Monthly-Cash-Inflow-Comparisons-ENS
-- query link: https://dune.com/queries/3440396


with entries as (
    select * from query_2244104
),

items as (
    select
        '1' as rk,
        'Cash In' as item,
        date_trunc('month', ts) as period,
        sum(case when cast(account as varchar) like '121%' then amount end)
            as amount
    from entries
    where
        ledger = 'CASH'
        and ts >= current_date - interval '400' day
    group by 3
)

select
    sum(
        case
            when period = date_trunc('month', current_date) then amount else 0
        end
    )
    / 1e6 as current_month_cash_inflow,
    sum(
        case
            when
                period = date_trunc('month', current_date) - interval '1' month
                then amount
            else 0
        end
    )
    / 1e6 as last_month_cash_inflow,
    sum(
        case
            when
                period = date_trunc('month', current_date) - interval '1' year
                then amount
            else 0
        end
    )
    / 1e6 as last_year_same_month_cash_inflow
from
    items
