-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: Revenues-ENS
-- query link: https://dune.com/queries/3440193


with entries as (
    select * from query_2244104
),
items as (
    select '1' as rk, 'Registration' as item, date_trunc('{{Time Period}}', ts) as period, sum(case when cast(account as varchar) like '3211%' then amount end) as amount
    from entries
    group by 3
    union all
    select '2' as rk, 'Renewal' as item, date_trunc('{{Time Period}}', ts) as period, sum(case when cast(account as varchar) like '3212%' then amount end) as amount
    from entries
    group by 3
    union all
    select '3' as rk, 'Short Name Claims' as item, date_trunc('{{Time Period}}', ts) as period, sum(case when cast(account as varchar) like '3213%' then amount end) as amount
    from entries
    group by 3
)
select *
from items
where period >= current_date - interval '365' day