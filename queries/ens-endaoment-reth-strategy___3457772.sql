-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: ENS-EnDAOment-RETH-Strategy
-- query link: https://dune.com/queries/3457772


WITH
  reth_wallets AS (
    SELECT 0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet,
      'ENS: EnDAOment' AS name /* likely the only one that will have it */
    UNION ALL
    SELECT
      0xfe89cc7abb2c4183683ab71653c4cdc9b02d44b7 AS wallet,
      'DAO Wallet' AS name
    UNION ALL
    SELECT
      0xcf60916b6cb4753f58533808fa610fcbd4098ec0 AS wallet,
      'ENS: Gnosis Safe' AS name
  ),
  reth_prices AS (
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
  ),
  reth_tokenflows AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "to" AS counterparty,
      contract_address,
      - CAST(value AS DOUBLE) AS qty,
      CAST(NULL AS DOUBLE) AS eth_qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN reth_wallets ON "from" = wallet
    WHERE
      contract_address = 0xae78736Cd615f374D3085123A210448E74Fc6393
    UNION ALL
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "from" AS counterparty,
      contract_address,
      CAST(value AS DOUBLE) AS qty,
      CAST(NULL AS DOUBLE) AS eth_qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN reth_wallets ON "to" = wallet
    WHERE
      contract_address = 0xae78736Cd615f374D3085123A210448E74Fc6393
    AND
      "from" <> 0x0000000000000000000000000000000000000000 --adding in mints below instead so mint fees can be accounted for
      
    UNION ALL
      
    SELECT
      mint.evt_block_time AS ts,
      mint.evt_tx_hash AS hash,
      wallet,
      0x0000000000000000000000000000000000000000 AS counterparty,
      0xae78736Cd615f374D3085123A210448E74Fc6393 AS contract_address,
      CAST(mint.amount AS DOUBLE) AS qty,
      CAST(dep.amount AS DOUBLE) AS eth_qty
    FROM rocketpool_ethereum.RocketTokenRETH_evt_TokensMinted mint
    INNER JOIN reth_wallets ON "to" = wallet
    LEFT JOIN rocketpool_ethereum.RocketDepositPool_evt_DepositReceived dep
    ON mint.evt_tx_hash = dep.evt_tx_hash
    AND mint.evt_index = dep.evt_index - 1
  ),
  reth_rates AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      NULL AS counterparty,
      0xae78736Cd615f374D3085123A210448E74Fc6393 AS contract_address,
      TRY_CAST(NULL AS DOUBLE) AS qty,
      TRY_CAST(NULL AS DOUBLE) AS eth_qty,
      CAST(reth_rt.totalEth AS DOUBLE)/NULLIF(reth_rt.rethSupply,CAST(0 AS UINT256)) AS rate
    FROM
      rocketpool_ethereum.RocketNetworkBalances_evt_BalancesUpdated reth_rt
      --this table starts in april 2023 which is fine for endaoment purposes. to go back further need to stitch with rocketnetwork_ethereum.RocketNetworkBalances_evt_BalancesUpdated
      CROSS JOIN reth_wallets
  ),
  reth_unioned AS (
    SELECT
      *,
      TRY_CAST(NULL AS DOUBLE) AS rate
    FROM
      reth_tokenflows
    UNION ALL
    SELECT
      *
    FROM
      reth_rates
  ),
  reth_info AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      contract_address,
      qty,
      eth_qty,
      qty * rate_filled AS equivalent_qty,
      (cum_qty) * (
        rate_filled - LAG(rate_filled) OVER (
          PARTITION BY
            wallet
          ORDER BY
            ts
        )
      ) AS reth_rebase_accrual,
      (cum_qty) * rate_filled AS reth_balance
    FROM
      (
        SELECT
          *,
          MAX(rate) OVER (
            PARTITION BY
              rate_grp
          ) AS rate_filled,
          SUM(qty) OVER (
            PARTITION BY
              wallet
            ORDER BY
              ts
          ) AS cum_qty
        FROM
          (
            SELECT
              *,
              SUM(
                CASE
                  WHEN NOT rate IS NULL THEN 1
                  ELSE 0
                END
              ) OVER (
                ORDER BY
                  ts
              ) AS rate_grp
            FROM
              reth_unioned
          ) AS sub
      ) AS sub2
    WHERE cum_qty IS NOT NULL
  ),
  reth_info_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,/* treating forms of steth as weth */ 
      equivalent_qty / 1e18 AS qty, --equivalent eth amount of deposits/withdrawals into reth
      eth_qty / 1e18 AS eth_qty, --actual sum deposited which seemingly includes a ~5 bps fee
      reth_rebase_accrual / 1e18 AS reth_rebase_accrual,
      price
    FROM
      reth_info
      JOIN reth_prices ON CAST(ts AS DATE) = period
  ),
  reth_balance AS (
    SELECT
      CAST(ts AS DATE) AS period,
      wallet,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      /* fungible with weth */ reth_balance / 1e18 AS qty
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              wallet,
              CAST(ts AS DATE)
            ORDER BY
              ts DESC
          ) AS rn
        FROM
          reth_info
      ) AS sub
    WHERE
      rn = 1
  ),
  reth_details_1 AS (
    SELECT
      period,
      wallet,
      contract_address,
      qty,
      price AS usd_price,
      qty * price AS usd_value
    FROM
      reth_balance
      INNER JOIN reth_prices USING (period, contract_address)
    WHERE
      qty > 0
  ),
  reth_details_lag AS (
    SELECT
      period,
      wallet,
      contract_address,
      qty,
      usd_price,
      CAST(usd_value AS DOUBLE),
      LAG(usd_price) OVER (
        PARTITION BY
          wallet
        ORDER BY
          period
      ) AS lag_price,
      LAG(qty) OVER (
        PARTITION BY
          wallet
        ORDER BY
          period
      ) AS lag_qty
    FROM
      reth_details_1
  ),
  reth_accounting AS (
    SELECT
      'M2M-RETH-' || CAST(RANK() OVER (
        ORDER BY
          period,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131007 AS account,
      'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-RETH-' || CAST(RANK() OVER (
        ORDER BY
          period,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32311 AS account,
      /* in the weth m2m account. can move to distinct m2m acct if theres a good reason */ 'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    /* Below we grab mints (/burns) of steth from eth and move them between the steth and eth accounts. swaps are taken care of in the swaps queries, transfers in the transfers query. */
    SELECT
      'ENTER-EXIT-RETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131007 AS account,
      'P&L' AS ledger,
      ts,
      qty * price AS amount,
      contract_address AS token_address,
      qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL
    UNION ALL
    SELECT
      'ENTER-EXIT-RETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      121 AS account,
      'P&L' AS ledger,
      ts,
      - qty * price AS amount,
      contract_address AS token_address,
      - qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL /* excluding wsteth mints/burns */
    UNION ALL
    /* Below we grab instant pnl of mints (mint fees)*/
    SELECT
      'INSTPNL-RETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      121 AS account,
      'P&L' AS ledger,
      ts,
      (qty - eth_qty) * price AS amount,
      contract_address AS token_address,
      qty - eth_qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND eth_qty IS NOT NULL
    UNION ALL
    SELECT
      'INSTPNL-RETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321010 AS account,
      'P&L' AS ledger,
      ts,
      (qty - eth_qty) * price AS amount,
      contract_address AS token_address,
      qty - eth_qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND eth_qty IS NOT NULL
    UNION ALL
    SELECT
      'ACCRUAL-RETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131007 AS account,
      'P&L' AS ledger,
      ts,
      reth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      reth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_info_2
    WHERE
      reth_rebase_accrual > 0
    UNION ALL
    SELECT
      'ACCRUAL-RETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321010 AS account,
      'P&L' AS ledger,
      ts,
      reth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      reth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      reth_info_2
    WHERE
      reth_rebase_accrual > 0
  )
SELECT
  *
FROM
  reth_accounting