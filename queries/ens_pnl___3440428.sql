-- part of a query repo
-- query name: PnL-ENS
-- query link: https://dune.com/queries/3440428


with entries as (
    select cast(account as varchar) as account, amount, date_trunc('{{Time Period}}', ts) as period from dune.outsider_analytics_team.result_ens_acc_main
),
items as (
    select '321' as rk, 'Revenues' as item, period, sum(case when account like '321%' then amount end) as amount
    from entries
    group by period
    union all
    select '322' as rk, 'Op. Expenses' as item, period, sum(case when account like '322%' then amount end) as amount
    from entries
    group by period
    union all
    select '3231' as rk, 'Currencies effect' as item, period, sum(case when account like '3231%' then amount end) as amount
    from entries
    group by period
    union all
    select '3232' as rk, 'Investments P&L' as item, period, sum(case when account like '3232%' or account like '3233%' then amount end) as amount
    from entries
    group by period
    union all
    select '5' as rk, 'P&L (excl. FX)' as item, period, sum(case when account like '32%' and account not like '3231%' then amount end) as amount
    from entries
    group by period
)
SELECT 
    period, 
    sum(CASE WHEN item = 'P&L (excl. FX)' OR item = 'Currencies effect' THEN amount END) as "Retained Earnings (including fx)",
    sum(CASE WHEN item = 'P&L (excl. FX)' THEN amount END) as "Retained Earnings (excluding fx)",
    sum(CASE WHEN item = 'Investments P&L' THEN amount END) as "Investment P&L",
    sum(CASE WHEN item = 'Revenues' THEN amount END) as "Op Revenues",
    -sum(CASE WHEN item = 'Op. Expenses' THEN -amount END) as "Op Expenses"
from items
where period >= current_date - interval '3' year
    -- and period < date_trunc('month', current_date)
GROUP BY
    period Expenses' THEN amount END) as "Operating Expenses"
from items
where period >= current_date - interval '3' year
    -- and period < date_trunc('month', current_date)
GROUP BY
    period