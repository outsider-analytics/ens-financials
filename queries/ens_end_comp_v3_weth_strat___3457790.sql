-- part of a query repo
-- query name: ENS-EnDAOment-Compound-WETH-V3-Strategy
-- query link: https://dune.com/queries/3457790


WITH comp_wallet_list AS (
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet 
  ),
  comp_contract_address_list_ethereum AS (
    SELECT
      0xa17581a9e3356d9a858b789d68b4d866e593ae94 AS contract_address, --cWETHv3 on mainnet
      18 AS underlying_decimals,
      'WETH' AS token,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address -- WETH on mainnet 
  ),
  comp_rates_raw_ethereum AS (
    -- Big thanks to @cryptokoryo for reference
    SELECT
      b."from" AS wallet,
      a.contract_address,
      a.evt_block_time AS ts,
      a.evt_tx_hash AS hash,
      -(a.amount * 1e0) AS underlying,
      -(b.amount * 1e0) AS delta_ctok
    FROM
      compound_v3_ethereum.cWETHv3_evt_Withdraw a
      LEFT JOIN compound_v3_ethereum.cWETHv3_evt_Transfer b ON a.evt_tx_hash = b.evt_tx_hash
      JOIN comp_contract_address_list_ethereum c ON a.contract_address = c.contract_address
    WHERE b.to = 0x0000000000000000000000000000000000000000
      AND a.evt_index + 1 = b.evt_index
      AND b.amount * 1e0 <> 0
    UNION ALL
    
    SELECT
      b.to as wallet,
      a.contract_address,
      a.evt_block_time AS ts,
      a.evt_tx_hash AS hash,
      (a.amount * 1e0) AS underlying,
      (b.amount * 1e0) AS delta_ctok
    FROM
      compound_v3_ethereum.cWETHv3_evt_Supply a
      LEFT JOIN compound_v3_ethereum.cWETHv3_evt_Transfer b ON a.evt_tx_hash = b.evt_tx_hash
      JOIN comp_contract_address_list_ethereum c ON a.contract_address = c.contract_address
    WHERE b."from" = 0x0000000000000000000000000000000000000000
      AND a.evt_index + 1 = b.evt_index
       AND b.amount * 1e0 <> 0
  ),
   aprrate_ethereum AS (
    select
     call_block_time,
      max(supply_rates.contract_address) as contract_address,
      max(output_0) * 1e-18 as Rate,
      max(utilization) * 1e18 as Util,
      ((max(output_0) * 1e-18) * 60 * 60 * 24 * 365) as APR
    from
    compound_v3_ethereum.cWETHv3_call_getSupplyRate as supply_rates
    JOIN comp_contract_address_list_ethereum as contract_list ON  supply_rates.contract_address = contract_list.contract_address
    group by 1
    order by call_block_time desc
    
    -- select date_trunc('day', time) as call_block_time, contract_address
    --     , AVG(supply_rate) as rate
    --     -- , AVG(borrow_rate) as borrow_rate
    -- FROM (
    --     SELECT time, contract_address
    --         , 24 * 365 * (base_supply_index - LAG(base_supply_index) OVER (ORDER BY time))/LAG(base_supply_index) OVER (ORDER BY time) as supply_rate
    --         , 24 * 365 * (base_borrow_index - LAG(base_borrow_index) OVER (ORDER BY time))/LAG(base_borrow_index) OVER (ORDER BY time) as borrow_rate
    --     FROM (
    --         SELECT date_trunc('hour', time) as time
    --         , 0xc3d688b66703497daa19211eedff47f25384cdc3 as contract_address
    --         , (max_by(base_supply_index, time)/1e15) as base_supply_index
    --         , (max_by(base_borrow_index, time)/1e15) as base_borrow_index
    --         FROM dune.steakhouse.dataset_steakhouse_compound_usdc_v3_rates as rates
    --         JOIN ethereum.blocks as eth_blocks
    --         ON rates.block_num = eth_blocks.number
    --         GROUP BY 1, 2
    --     )
    -- )
    -- GROUP BY 1, 2
  )
  
  , addrate as ( 
    select
       a.wallet, a.ts , a.hash, a.delta_ctok, a.contract_address, a.underlying as delta_underlying, case when date_trunc('hour', a.ts) = date_trunc('hour', b.call_block_time) then b.Rate else null end as Rate
    from
      comp_rates_raw_ethereum a
      left join aprrate_ethereum b on a.contract_address = b.contract_address 
      group by 1,2,3,4,5,6,7 
)
, competh_prices as (
    SELECT
      DATE_TRUNC('day', minute) AS period,
      contract_address,
      price
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      AND EXTRACT(
        hour
        FROM
          minute
      ) = 23
      AND EXTRACT(
        minute
        FROM
          minute
      ) = 59


), maxrate AS ( 
    with data AS ( 
    select  a.wallet, a.ts , a.hash , a.delta_ctok , a.contract_address , a.delta_underlying  , max(a.Rate) as rate,
    SUM(CASE WHEN NOT max(a.Rate) IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY a.contract_address ORDER BY ts NULLS FIRST) AS rate_grp
    from addrate a
    group by 1,2,3,4,5,6
    )
      select * , case when rate_grp = 0 then 1e-10 else MAX(rate) OVER (PARTITION BY contract_address, rate_grp) end AS rate_filled
      from data 
  ),
  comp_with_rates AS (
    SELECT
      *
    FROM
      maxrate
      JOIN comp_wallet_list USING (wallet)
  ),
  comp_min_ts AS (
    SELECT
      wallet,
      contract_address,
      MIN(ts) AS min_ts
    FROM
      comp_with_rates
    GROUP BY
      1,
      2
  ),
   comp_rates AS (
    SELECT
      wallet,
      ts,
      hash,
      NULL AS delta_ctok,
      sub.contract_address,
      NULL AS delta_underlying,
      rate_filled
    FROM
      (
        SELECT
          contract_address,
          ts,
          hash,
          rate_filled,
          ROW_NUMBER() OVER (
            PARTITION BY
              contract_address,
              CAST(ts AS DATE)
            ORDER BY
              ts DESC
          ) AS rn
        FROM
          maxrate
      ) AS sub
      JOIN comp_min_ts ON ts > min_ts 
      AND sub.contract_address = comp_min_ts.contract_address
      AND rn = 1
  ),
  comp_unioned AS (
  
  select wallet, ts,hash,delta_ctok,contract_address, delta_underlying,cumulative_ctok,cumulative_underlying,previous_ctok
  ,rate_filled,dailyrate,
  case when interest_accrual < 0 then 0 else interest_accrual end as interest_accrual, 
  case when interest_accrual >= 0 then SUM(interest_accrual) OVER (PARTITION BY wallet,contract_address ORDER BY ts NULLS FIRST) else 0 end as cumulative_interest
  from (
    SELECT
      wallet,
      ts,
      hash,
      delta_ctok,
      contract_address,
      delta_underlying,
      cumulative_ctok,
      cumulative_underlying,
      cumulative_ctok - COALESCE(delta_ctok, 0) AS previous_ctok,
      rate_filled,
      rate_filled * 60 * 60 * 24 as dailyrate,
      case when cumulative_underlying >= 0 then (cumulative_underlying - COALESCE(delta_underlying, 0)) * rate_filled * 60  * (date_diff('minute', cast(LAG(ts) over (PARTITION BY wallet,contract_address ORDER BY ts) AS timestamp) ,  cast(ts AS timestamp))) else 0 end as interest_accrual
      
    FROM
      (
        SELECT
          *,
          SUM(delta_ctok) OVER (
            PARTITION BY
              wallet,
              contract_address
            ORDER BY
              ts NULLS FIRST
          ) AS cumulative_ctok,
          SUM(delta_underlying) OVER (
            PARTITION BY
              wallet,
              contract_address
            ORDER BY
              ts NULLS FIRST
          ) AS cumulative_underlying
        FROM
          (
            SELECT
              wallet,ts,hash,delta_ctok,contract_address,delta_underlying,rate_filled
            FROM
              comp_with_rates
            UNION ALL
            SELECT
              wallet,ts,hash,delta_ctok,contract_address,delta_underlying,rate_filled
            FROM
              comp_rates
          ) AS sub
      ) AS sub2
  )), 
    comp_accounting AS (
    SELECT
      'ENTER-EXIT-COMP-' || token || '-' || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          ts NULLS FIRST,
          delta_underlying NULLS FIRST
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token = 'WETH' THEN 131010 /* increase in money market assets COMP WETH */
      END AS account,
      'P&L' AS ledger,
      ts,
      delta_underlying / POWER(10, underlying_decimals) AS amount,
     token_address,
      delta_underlying / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list_ethereum USING (contract_address)
    WHERE
      NOT delta_underlying IS NULL
    UNION ALL
    SELECT
      'ENTER-EXIT-COMP-' || token || '-' || CAST (ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          ts NULLS FIRST,
          delta_underlying NULLS FIRST
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token = 'WETH' THEN 121 /* decrease in cash assets */
        -- WHEN token = 'DAI' THEN 111002
      END AS account,
      'P&L' AS ledger,
      ts,
      - delta_underlying / POWER(10, underlying_decimals) AS amount,
      token_address,
      - delta_underlying / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list_ethereum USING (contract_address)
    WHERE
      NOT delta_underlying IS NULL
    UNION ALL
    SELECT
        format('ACCRUAL-COMP-%s-%d', token, 
        ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          ts NULLS FIRST,
          interest_accrual NULLS FIRST
        )
      )
      AS transaction,
      1 AS line,
      CASE
        WHEN token = 'WETH' THEN 131010 /* increase in money market assets COMP WETH*/
      END AS account,
      'P&L' AS ledger,
      ts,
      interest_accrual / POWER(10, underlying_decimals) AS amount,
     token_address,
      interest_accrual / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list_ethereum USING (contract_address)
    WHERE
      COALESCE(interest_accrual, 0) <> 0
    UNION ALL
    SELECT
        format('ACCRUAL-COMP-%s-%d', token, 
        ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          ts NULLS FIRST,
          interest_accrual NULLS FIRST
        )
      ) AS transaction,
      2 AS line,
      CASE
        WHEN token = 'WETH' THEN 32321013 /* increase in investment P&L COMP WETH */
      END AS account,
      'P&L' AS ledger,
      ts,
      interest_accrual / POWER(10, underlying_decimals) AS amount,
       token_address,
      interest_accrual / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list_ethereum USING (contract_address)
    WHERE
      COALESCE(interest_accrual, 0) <> 0
  )
SELECT
  transaction, line, account, ledger, ts, competh_prices.price * amount as amount, token_address, token_amount, reference, wallet
FROM
  comp_accounting
 JOIN competh_prices on date_trunc('day', comp_accounting.ts) = competh_prices.period AND comp_accounting.token_address = competh_prices.contract_address
