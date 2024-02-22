-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: Endowment-Assets-ENS
-- query link: https://dune.com/queries/3451107


with entries as (
    select cast(account as varchar) account, ts, amount from dune.steakhouse.result_ens_accounting_main
    where wallet = '0x4f2083f5fbede34c2714affb3105539775f7fe64'
        and cast(account as varchar) like '1%' 
),
accounts as (   
    select * from query_2181835
),
items as (
    select account, date(ts) as period, amount as amount
    from entries
    union all
    select account, period, null as amount
    from (select distinct account from entries) items
    cross join (select distinct date(ts) as period from entries) periods
),
group_by as (
    select account, period, sum(amount) as amount
    from items
    group by 1, 2
),
balances as (
    select account, period, sum(amount) over (partition by account order by period asc) as balance
    from group_by
)

select coalesce(account_label, accounts.account) as item, 
       cast(date_trunc('{{Time Period}}', period) as timestamp) as period, 
       AVG(case when abs(balance) < 1 then 0 else balance end) as balance
from balances
left join accounts on balances.account = accounts.account
GROUP BY coalesce(account_label, accounts.account), cast(date_trunc('{{Time Period}}', period) as timestamp)
order by item asc
