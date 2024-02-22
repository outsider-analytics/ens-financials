-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: Cash-Inflow-ENS
-- query link: https://dune.com/queries/3440372


with entries as (
    select * from query_2244104
),
items as (
    select '1' as rk, 'Cash In' as item, date_trunc('{{Time Period}}', ts) as period, sum(case when cast(account as varchar) like '121%' then amount end) as amount
    from entries 
    where ledger = 'CASH'
    AND ts >= current_date - interval '365' day
    group by 3
)
select *
from items
