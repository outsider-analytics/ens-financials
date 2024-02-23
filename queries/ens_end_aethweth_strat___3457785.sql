-- part of a query repo
-- query name: ENS-EnDAOment-aEthWETH-Strategy
-- query link: https://dune.com/queries/3457785


WITH
  aethweth_wallets AS (
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
  aethweth_prices AS (
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
  aethweth_tokenflows AS (
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
      INNER JOIN aethweth_wallets ON "from" = wallet
    WHERE
      contract_address = 0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8
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
      INNER JOIN aethweth_wallets ON "to" = wallet
    WHERE
      contract_address = 0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8
  ),
  aethweth_rates AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      NULL AS counterparty,
      0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8 AS contract_address,
      TRY_CAST(NULL AS DOUBLE) AS qty,
      TRY_CAST(NULL AS DOUBLE) AS eth_qty,
    --   POWER(1 + (liquidityRate * 1e-27)/31536000, 31536000) - 1
      
      liquidityIndex * 1e-27 AS rate
    FROM aave_v3_ethereum.Pool_evt_ReserveDataUpdated 
      CROSS JOIN aethweth_wallets
    WHERE reserve = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
  ),
  aethweth_unioned AS (
    SELECT
      *,
      TRY_CAST(NULL AS DOUBLE) AS rate
    FROM
      aethweth_tokenflows
    UNION ALL
    SELECT
      *
    FROM
      aethweth_rates
  ),
  aethweth_info AS (
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
      ) AS aethweth_rebase_accrual,
      (cum_qty) * rate_filled AS aethweth_balance
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
              aethweth_unioned
          ) AS sub
      ) AS sub2
    WHERE cum_qty IS NOT NULL
  ),
  aethweth_info_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,/* treating forms of aethweth as weth */ 
      qty / 1e18 AS qty, --equivalent eth amount of deposits/withdrawals into aethweth -- Fix by equivalent_qty => qty
      eth_qty / 1e18 AS eth_qty, --actual sum deposited which seemingly includes a ~5 bps fee
      aethweth_rebase_accrual / 1e18 AS aethweth_rebase_accrual,
      price
    FROM
      aethweth_info
      JOIN aethweth_prices ON CAST(ts AS DATE) = period
  ),
  aethweth_balance AS (
    SELECT
      CAST(ts AS DATE) AS period,
      wallet,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      /* fungible with weth */ aethweth_balance / 1e18 AS qty
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
          aethweth_info
      ) AS sub
    WHERE
      rn = 1
  ),
  aethweth_details_1 AS (
    SELECT
      period,
      wallet,
      contract_address,
      qty,
      price AS usd_price,
      qty * price AS usd_value
    FROM
      aethweth_balance
      INNER JOIN aethweth_prices USING (period, contract_address)
    WHERE
      qty > 0
  ),
  aethweth_details_lag AS (
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
      aethweth_details_1
  ),
  aethweth_accounting AS (
    SELECT
      'M2M-AETHWETH-' || CAST(RANK() OVER (
        ORDER BY
          period,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131012 AS account,
      'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aethweth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-AETHWETH-' || CAST(RANK() OVER (
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
      aethweth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    /* Below we grab mints (/burns) of steth from eth and move them between the steth and eth accounts. swaps are taken care of in the swaps queries, transfers in the transfers query. */
    SELECT
      'ENTER-EXIT-AETHWETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131012 AS account,
      'P&L' AS ledger,
      ts,
      qty * price AS amount,
      contract_address AS token_address,
      qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aethweth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL
    UNION ALL
    SELECT
      'ENTER-EXIT-AETHWETH-' || CAST(RANK() OVER (
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
      aethweth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL /* excluding wsteth mints/burns */
    UNION ALL
    SELECT
      'ACCRUAL-AETHWETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131012 AS account,
      'P&L' AS ledger,
      ts,
      aethweth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      aethweth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aethweth_info_2
    WHERE
      aethweth_rebase_accrual > 0
    UNION ALL
    SELECT
      'ACCRUAL-AETHWETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321015 AS account,
      'P&L' AS ledger,
      ts,
      aethweth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      aethweth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aethweth_info_2
    WHERE
      aethweth_rebase_accrual > 0
  )
SELECT
  *
FROM
  aethweth_accountingaccrual > 0
  )
SELECT
  *
FROM
  aethweth_accounting