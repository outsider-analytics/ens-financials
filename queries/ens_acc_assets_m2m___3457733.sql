-- part of a query repo
-- query name: ENS-Accounting-Assets-M2M
-- query link: https://dune.com/queries/3457733


WITH
  m2m_wallets AS (
    SELECT
      0xfe89cc7abb2c4183683ab71653c4cdc9b02d44b7 AS wallet,
      'ENS: DAO Wallet' AS name
    UNION ALL
    SELECT
      0xcf60916b6cb4753f58533808fa610fcbd4098ec0 AS wallet,
      'ENS: Gnosis Safe' AS name
    UNION ALL
    SELECT
      0x911143d946ba5d467bfc476491fdb235fef4d667 AS wallet,
      'ENS: Multisig' AS name
    UNION ALL
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet,
      'ENS: EnDAOment' AS name
    UNION ALL
    SELECT
      contract_address AS wallet,
      'ENS: Registrar Controller ' || TRY_CAST(
        ROW_NUMBER() OVER (
          ORDER BY
            min_ts NULLS FIRST
        ) AS VARCHAR
      ) AS name
    FROM
      (
        SELECT
          contract_address,
          MIN(evt_block_time) AS min_ts
        FROM
          ens.view_registrations
        GROUP BY
          1
      ) AS sub
  ),
  m2m_tokens AS (
    SELECT
      contract_address,
      symbol,
      decimals,
      POWER(10, decimals) AS divisor
    FROM
      tokens.erc20
    WHERE
      blockchain = 'ethereum'
      AND contract_address IN (
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
        0xfe2e637202056d30016725477c5da089ab0a043a
      )
  ),
  m2m_prices AS (
    SELECT
      DATE_TRUNC('day', minute) AS period,
      contract_address,
      price
    FROM
      prices.usd
      INNER JOIN m2m_tokens USING (contract_address)
    WHERE
      blockchain = 'ethereum'
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
      AND NOT contract_address IN (
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
        0xfe2e637202056d30016725477c5da089ab0a043a
      )
    UNION ALL
    SELECT
      period,
      0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS contract_address,
      1.0 AS price
    FROM
      UNNEST (
        SEQUENCE(
          CAST(
            SUBSTR(CAST('2018-01-01' AS VARCHAR), 1, 10) AS DATE
          ),
          CURRENT_DATE - INTERVAL '1' day,
          INTERVAL '1' day
        )
      ) AS _u (period)
  ),
  m2m_tokenflows AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "to" AS counterparty,
      contract_address,
      - CAST(value AS DOUBLE) AS qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN m2m_tokens USING (contract_address)
      INNER JOIN m2m_wallets ON "from" = wallet
    UNION ALL
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "from" AS counterparty,
      contract_address,
      CAST(value AS DOUBLE) AS qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN m2m_tokens USING (contract_address)
      INNER JOIN m2m_wallets ON "to" = wallet
    UNION ALL
    SELECT
      block_time AS ts,
      tx_hash AS hash,
      wallet,
      "to" AS counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      - CAST(value AS DOUBLE) AS qty
    FROM
      ethereum.traces
      INNER JOIN m2m_wallets ON "from" = wallet
    WHERE
      success = TRUE
      AND (
        NOT call_type IN ('delegatecall', 'callcode', 'staticcall')
        OR call_type IS NULL
      )
      AND NOT to IN (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2) /* WETH, doesn't have ERC20 mint */
    UNION ALL
    SELECT
      block_time AS ts,
      tx_hash AS hash,
      wallet,
      "from" AS counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      CAST(value AS DOUBLE) AS qty
    FROM
      ethereum.traces
      INNER JOIN m2m_wallets ON "to" = wallet
    WHERE
      success = TRUE
      AND (
        NOT call_type IN ('delegatecall', 'callcode', 'staticcall')
        OR call_type IS NULL
      )
      AND NOT "from" IN (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2) /* WETH, doesn't have ERC20 burn */
    UNION ALL
    SELECT
      ts,
      NULL AS hash,
      wallet,
      NULL AS counterparty,
      contract_address,
      NULL AS qty
    FROM
      m2m_wallets
      CROSS JOIN m2m_tokens
      CROSS JOIN UNNEST (
        SEQUENCE(
          CAST(
            SUBSTR(CAST('2018-01-01' AS VARCHAR), 1, 10) AS DATE
          ),
          CURRENT_DATE - INTERVAL '1' day,
          INTERVAL '1' day
        )
      ) AS _u (ts)
  ),
  m2m_grp_asset_period AS (
    SELECT
      CAST(ts AS DATE) AS period,
      wallet,
      contract_address,
      SUM(qty) AS qty
    FROM
      m2m_tokenflows
    WHERE
      COALESCE(qty, 900) + 0 <> 0 /* accept nulls, don't accept 0 */
    GROUP BY
      1,
      2,
      3
  ),
  m2m_balance AS (
    SELECT
      period,
      wallet,
      contract_address AS token_address,
      CASE
        WHEN contract_address IN (0xfe2e637202056d30016725477c5da089ab0a043a) THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE contract_address
      END AS contract_address,
      /* equating forms of stETH to WETH */ symbol,
      qty / divisor AS delta,
      SUM(qty) OVER (
        PARTITION BY
          wallet,
          contract_address
        ORDER BY
          period NULLS FIRST
      ) / divisor AS qty
    FROM
      m2m_grp_asset_period
      INNER JOIN m2m_tokens USING (contract_address)
  ),
  m2m_details_1 AS (
    SELECT
      period,
      wallet,
      token_address,
      contract_address,
      symbol,
      delta,
      qty,
      price AS usd_price,
      qty * price AS usd_value
    FROM
      m2m_balance
      INNER JOIN m2m_prices USING (period, contract_address)
    WHERE
      NOT qty IS NULL
  ),
  m2m_details_lag AS (
    SELECT
      period,
      wallet,
      token_address,
      contract_address,
      symbol,
      delta,
      qty,
      usd_price,
      CAST(usd_value AS DOUBLE),
      LAG(usd_price) OVER (
        PARTITION BY
          wallet,
          token_address
        ORDER BY
          period NULLS FIRST
      ) AS lag_price,
      LAG(qty) OVER (
        PARTITION BY
          wallet,
          token_address
        ORDER BY
          period NULLS FIRST
      ) AS lag_qty
    FROM
      m2m_details_1
  ),
  m2m_accounting AS (
    SELECT
      'M2M-' || CAST(RANK() OVER (
        ORDER BY
          period NULLS FIRST,
          wallet NULLS FIRST,
          token_address NULLS FIRST
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121
        WHEN token_address = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 131005
        ELSE 999999
      END AS account,
      'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      /* sets all forms of steth as weth here */ 0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      m2m_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-' || CAST (RANK() OVER (
        ORDER BY
          period NULLS FIRST,
          wallet NULLS FIRST,
          token_address NULLS FIRST
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 32311 /* weth */
        WHEN token_address = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 32311 /* seth2 */
        ELSE 32311 /* eth m2m */
      END AS account,
      /* in case we want to put these in diff accounts later */ 'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      m2m_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
  ),
  m2m_seth2_accounting AS (
    /* Below we grab mints (/burns) of seth2 from eth and move them between the steth and eth accounts. swaps are taken care of in the swaps queries, transfers in the transfers query. */
    SELECT
      'ENTER-EXIT-SETH2-' || CAST(RANK() OVER (
        ORDER BY
          ts NULLS FIRST,
          wallet NULLS FIRST
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131005 AS account,
      'P&L' AS ledger,
      ts,
      qty / divisor * price AS amount,
      m2m_prices.contract_address AS token_address,
      /* weth */ qty / divisor AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      m2m_tokenflows
      JOIN m2m_tokens ON m2m_tokenflows.contract_address = m2m_tokens.contract_address
      JOIN m2m_prices ON CAST(ts AS DATE) = period
      AND m2m_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND m2m_tokenflows.contract_address = 0xfe2e637202056d30016725477c5da089ab0a043a
    UNION ALL
    SELECT
      'ENTER-EXIT-SETH2-' || CAST (RANK() OVER (
        ORDER BY
          ts NULLS FIRST,
          wallet NULLS FIRST
      ) AS VARCHAR) AS transaction,
      2 AS line,
      121 AS account,
      'P&L' AS ledger,
      ts,
      - qty / divisor * price AS amount,
      m2m_prices.contract_address AS token_address,
      /* weth */ - qty / divisor AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      m2m_tokenflows
      JOIN m2m_tokens ON m2m_tokenflows.contract_address = m2m_tokens.contract_address
      JOIN m2m_prices ON CAST(ts AS DATE) = period
      AND m2m_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND m2m_tokenflows.contract_address = 0xfe2e637202056d30016725477c5da089ab0a043a
  )
SELECT
  *
FROM
  m2m_accounting
UNION ALL
SELECT
  *
FROM
  m2m_seth2_accounting
UNION ALL
SELECT
  *
FROM
  m2m_seth2_accounting