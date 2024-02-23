-- part of a query repo
-- query name: ENS-EnDAOment-ETHx-Strategy
-- query link: https://dune.com/queries/3457779


WITH
  ethx_wallets AS (
    SELECT 0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet,
      'ENS: EnDAOment' AS name /* likely the only one that will have it */
  ),
  ethx_prices AS (
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
  ethx_tokenflows AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "to" AS counterparty,
      contract_address,
      - CAST(value AS DOUBLE) AS qty,
      NULL AS eth_qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN ethx_wallets ON "from" = wallet
    WHERE
      contract_address = 0xA35b1B31Ce002FBF2058D22F30f95D405200A15b
    UNION ALL
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "from" AS counterparty,
      contract_address,
      CAST(value AS DOUBLE) AS qty,
      NULL AS eth_qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN ethx_wallets ON "to" = wallet
    WHERE
      contract_address = 0xA35b1B31Ce002FBF2058D22F30f95D405200A15b
  ),
  ethx_rates AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      NULL AS counterparty,
      0xA35b1B31Ce002FBF2058D22F30f95D405200A15b AS contract_address,
      TRY_CAST(NULL AS DOUBLE) AS qty,
      TRY_CAST(NULL AS DOUBLE) AS eth_qty,
      CAST(ethx_rt.totalEth AS DOUBLE)/NULLIF(ethx_rt.ethxSupply, CAST(0 AS UINT256)) AS rate
    FROM
      stader_ethereum.StaderOracle_evt_ExchangeRateUpdated ethx_rt
      
      --this table starts in april 2023 which is fine for endaoment purposes. to go back further need to stitch with rocketnetwork_ethereum.RocketNetworkBalances_evt_BalancesUpdated
      CROSS JOIN ethx_wallets
  ),
  ethx_unioned AS (
    SELECT
      *,
      TRY_CAST(NULL AS DOUBLE) AS rate
    FROM
      ethx_tokenflows
    UNION ALL
    SELECT
      *
    FROM
      ethx_rates
  ),
  ethx_info AS (
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
      ) AS ethx_rebase_accrual,
      (cum_qty) * rate_filled AS ethx_balance
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
              ethx_unioned
          ) AS sub
      ) AS sub2
    WHERE cum_qty IS NOT NULL
  ),
  ethx_info_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,/* treating forms of ethx as weth */ 
      equivalent_qty / 1e18 AS qty, --equivalent eth amount of deposits/withdrawals into ethx
      eth_qty / 1e18 AS eth_qty, --actual sum deposited which seemingly includes a ~5 bps fee
      ethx_rebase_accrual / 1e18 AS ethx_rebase_accrual,
      price
    FROM
      ethx_info
      JOIN ethx_prices ON CAST(ts AS DATE) = period
  ),
  ethx_balance AS (
    SELECT
      CAST(ts AS DATE) AS period,
      wallet,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      /* fungible with weth */ ethx_balance / 1e18 AS qty
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
          ethx_info
      ) AS sub
    WHERE
      rn = 1
  ),
  ethx_details_1 AS (
    SELECT
      period,
      wallet,
      contract_address,
      qty,
      price AS usd_price,
      qty * price AS usd_value
    FROM
      ethx_balance
      INNER JOIN ethx_prices USING (period, contract_address)
    WHERE
      qty > 0
  ),
  ethx_details_lag AS (
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
      ethx_details_1
  ),
  ethx_accounting AS (
    SELECT
      'M2M-ETHX-' || CAST(RANK() OVER (
        ORDER BY
          period,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131009 AS account,
      'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ethx_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-ETHX-' || CAST(RANK() OVER (
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
      ethx_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    /* Below we grab mints (/burns) of steth from eth and move them between the steth and eth accounts. swaps are taken care of in the swaps queries, transfers in the transfers query. */
    SELECT
      'ENTER-EXIT-ETHX-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131009 AS account,
      'P&L' AS ledger,
      ts,
      qty * price AS amount,
      contract_address AS token_address,
      qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ethx_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL
    UNION ALL
    SELECT
      'ENTER-EXIT-ETHX-' || CAST(RANK() OVER (
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
      ethx_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL /* excluding wsteth mints/burns */
    UNION ALL
    /* Below we grab instant pnl of mints (mint fees)*/
    SELECT
      'INSTPNL-ETHX-' || CAST(RANK() OVER (
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
      ethx_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND eth_qty IS NOT NULL
    UNION ALL
    SELECT
      'INSTPNL-ETHX-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321012 AS account,
      'P&L' AS ledger,
      ts,
      (qty - eth_qty) * price AS amount,
      contract_address AS token_address,
      qty - eth_qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ethx_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND eth_qty IS NOT NULL
    UNION ALL
    SELECT
      'ACCRUAL-ETHX-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131009 AS account,
      'P&L' AS ledger,
      ts,
      ethx_rebase_accrual * price AS amount,
      contract_address AS token_address,
      ethx_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ethx_info_2
    WHERE
      ethx_rebase_accrual > 0
    UNION ALL
    SELECT
      'ACCRUAL-ETHX-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321012 AS account,
      'P&L' AS ledger,
      ts,
      ethx_rebase_accrual * price AS amount,
      contract_address AS token_address,
      ethx_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ethx_info_2
    WHERE
      ethx_rebase_accrual > 0
  )
SELECT
  *
FROM
  ethx_accounting