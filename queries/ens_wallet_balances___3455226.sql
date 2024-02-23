-- part of a query repo
-- query name: Wallet-Balances-ENS
-- query link: https://dune.com/queries/3455226


with wallets as (
    select
        0x283af0b28c62c092c9727f1ee09c02ca627eb7f5 as wallet,
        'ETH Registrar' as name
    union all
    select
        0xfe89cc7abb2c4183683ab71653c4cdc9b02d44b7 as wallet,
        'DAO Wallet' as name
),

tokens as (
    select
        contract_address,
        symbol,
        decimals,
        power(10, decimals) as divisor
    from tokens.erc20
    where
        blockchain = 'ethereum'
        and contract_address in (
            0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, -- USDC
            0xc18360217d8f7ab5e7c516566761ea12ce7f9d72, -- ENS
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
        )
),

tokenflows as (
    select
        evt_block_time as ts,
        wallet,
        to as counterparty,
        contract_address,
        symbol,
        -cast(value as double) as qty
    from erc20_ethereum.evt_transfer
    inner join tokens on evt_transfer.contract_address = tokens.contract_address
inner join wallets on "from" = wallet
union all
select
    evt_block_time as ts,
    wallet,
    "from" as counterparty,
    contract_address,
    symbol,
    cast(value as double) as qty
from erc20_ethereum.evt_transfer
inner join tokens on evt_transfer.contract_address = tokens.contract_address
inner join wallets on to = wallet
union all
select
block_time as ts,
wallet,
to as counterparty,
0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address,
'WETH' as symbol,
-cast(value as double) as qty
from ethereum.traces
inner join wallets on "from" = wallet
where
success = TRUE
and (
    call_type not in ('delegatecall', 'callcode', 'staticcall')
    or call_type is NULL
)
-- WETH, doesn't have ERC20 mint
and to not in (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2)
union all
select
block_time as ts,
wallet,
"from" as counterparty,
0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address,
'WETH' as symbol,
cast(value as double) as qty
from ethereum.traces
inner join wallets on to = wallet
where
success = TRUE
and (
    call_type not in ('delegatecall', 'callcode', 'staticcall')
    or call_type is NULL
)
-- WETH, doesn't have ERC20 burn
and "from" not in (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2)
union all
select
period as ts,
wallet,
NULL as counterparty,
contract_address,
symbol,
NULL as qty
from
unnest(
    sequence(
        date('2020-01-01'), current_date - interval '1' day, interval '1' day
    )
)
cross join wallets
cross join tokens
),

grp_asset_period as (
select
cast(ts as date) as period,
wallet,
contract_address,
symbol,
sum(qty) as qty
from tokenflows
group by 1, 2, 3, 4
),

balance as (
select
period,
wallet,
contract_address,
symbol,
qty as delta,
sum(qty) over (partition by wallet, contract_address order by period asc) as qty
from grp_asset_period
),

prices as (
select
contract_address,
price,
date_trunc('day', minute) as period
from prices.usd
where
blockchain = 'ethereum'
and extract(hour from minute) = 23
and extract(minute from minute) = 59
),

details as (
select
period,
wallet,
contract_address,
delta,
qty,
price as usd_price,
qty * price as usd_value
from balance
inner join
prices
on balance.period = prices.period and balance.contract_address = prices.contract_address
)

select
name,
symbol,
qty / divisor as qty,
usd_price as usd_price,
usd_value / divisor as usd_value
from details
inner join wallets on details.wallet = wallets.wallet
inner join tokens using (contract_address)
where period = current_date - interval '1' day
order by 1 asc, 2 asc
urrent_date - interval '1' day
order by 1 asc, 2 asc
