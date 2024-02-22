-- part of a query repo
-- query name: Monthly-Revenue-Comparisons-ENS
-- query link: https://dune.com/queries/3440279


with entries as (
    select * from query_2244104
),

items as (
    select
        '1' as rk,
        'Registration' as item,
        date_trunc('month', ts) as period,
        sum(case when cast(account as varchar) like '3211%' then amount end)
            as amount
    from entries
    where ts >= current_date - interval '400' day
    group by 3
    union all
    select
        '2' as rk,
        'Renewal' as item,
        date_trunc('month', ts) as period,
        sum(case when cast(account as varchar) like '3212%' then amount end)
            as amount
    from entries
    where ts >= current_date - interval '400' day
    group by 3
    union all
    select
        '3' as rk,
        'Short Name Claims' as item,
        date_trunc('month', ts) as period,
        sum(case when cast(account as varchar) like '3213%' then amount end)
            as amount
    from entries
    where ts >= current_date - interval '400' day
    group by 3
)

select
    sum(
        case
            when period = date_trunc('month', current_date) then amount else 0
        end
    )
    / 1e6 as current_month_revenue,
    sum(
        case
            when
                period = date_trunc('month', current_date) - interval '1' month
                then amount
            else 0
        end
    )
    / 1e6 as last_month_revenue,
    sum(
        case
            when
                period = date_trunc('month', current_date) - interval '1' year
                then amount
            else 0
        end
    )
    / 1e6 as last_year_same_month_revenue
from
    items
