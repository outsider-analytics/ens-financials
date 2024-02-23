-- part of a query repo
-- query name: Reserves-ENS
-- query link: https://dune.com/queries/3450989


with entries as (
    select cast(account as varchar) as account, amount, ts from dune.outsider_analytics_team.result_ens_acc_main
),
items as (
    SELECT 'Assets - USD' as item, date_trunc('{{Time Period}}', ts) as period, sum(case when account like '11%' then amount end) as amount
    from entries
    group by date_trunc('{{Time Period}}', ts)
    union all
    SELECT 'Assets - ETH' as item, date_trunc('{{Time Period}}', ts) as period, sum(case when account like '12%' or account like '13%' then amount end) as amount
    from entries
    group by date_trunc('{{Time Period}}', ts)
),
balances as (
    select item, period, sum(amount) over (partition by item order by period asc) as balance
    from items
)
SELECT 
    item, 
    cast(period as timestamp) as period, 
    AVG(balance) as balance
from balances
where period > current_date - interval '3' year
GROUP BY
    item, period