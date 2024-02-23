-- part of a query repo
-- query name: Operating-Expenses-ENS
-- query link: https://dune.com/queries/3440761


with entries as (
    select
        cast(account as varchar) as account,
        amount,
        date_trunc('month', ts) as period
    from dune.outsider_analytics_team.result_ens_acc_main
),

monthly_entries as (
    select
        period,
        amount,
        account
    from
        entries
    where
        account like '322%'
),

periods as (
    select distinct period from entries
),

cross_join as (
    select
        p.period as base_period,
        m.period as prev_period,
        m.amount,
        12
        - (extract(year from p.period) - extract(year from m.period)) * 12
        - (extract(month from p.period) - extract(month from m.period))
            as weight
    from
        periods as p
    inner join
        monthly_entries as m
        on p.period >= m.period and m.period > date_add('month', -12, p.period)
),

weighted_sums as (
    select
        base_period,
        -sum(case when base_period = prev_period then amount end)
            as unweighted_average,
        -sum(amount * weight) / 78 as weighted_average
    from
        cross_join
    group by
        base_period
)

select
    '322' as rk,
    'Op. Expenses' as item,
    base_period as period,
    weighted_average as "Normalized Cash Burn",
    unweighted_average as "Actual Cash Burn"
from
    weighted_sums
order by
    base_period;
rn"
from
    weighted_sums
order by
    base_period;
