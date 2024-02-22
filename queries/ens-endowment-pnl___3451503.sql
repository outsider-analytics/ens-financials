-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: Endowment-PnL-ENS
-- query link: https://dune.com/queries/3451503


with entries as (
    select cast(account as varchar) as account, amount, date_trunc('{{Time Period}}', ts) as period from dune.steakhouse.result_ens_accounting_main
    WHERE wallet = '0x4f2083f5fbede34c2714affb3105539775f7fe64' --This is the Endowment Wallet Address
),
items as (
    select '3231' as rk, 'Mark-to-market' as item, period, sum(case when account like '3231%' then amount end) as amount
    from entries
    group by period
    union all
    select '3232' as rk, 'Invest P&L' as item, period, sum(case when account like '3232%' then amount end) as amount
    from entries
    group by period
    union all
    select '32323' as rk, 'Swaps' as item, period, sum(case when account like '3233%' then amount end) as amount
    from entries
    group by period
)
select item, period, amount
from items
where period >= current_date - interval '3' year
    -- and period < date_trunc('month', current_date)
order by rk asc