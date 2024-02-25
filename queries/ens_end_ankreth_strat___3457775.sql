-- part of a query repo
-- query name: ENS-EnDAOment-ankrETH-Strategy
-- query link: https://dune.com/queries/3457775

WITH
  ankreth_wallets AS (
    SELECT 0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet,
      'ENS: EnDAOment' AS name /* likely the only one that will have it */
  ),
  ankreth_prices AS (
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
  ankreth_tokenflows AS (
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
      INNER JOIN ankreth_wallets ON "from" = wallet
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
      INNER JOIN ankreth_wallets ON "to" = wallet
    WHERE
      contract_address = 0xA35b1B31Ce002FBF2058D22F30f95D405200A15b
  ),
  ankreth_rates AS (
    WITH ankreth_supply as (
        SELECT date_series.time
            , SUM(SUM(COALESCE(amount, 0))) OVER (ORDER BY date_series.time) as supply
        FROM (
            SELECT date_trunc('day', evt_block_time) as time, value * 1e-18 as amount
            FROM ankr_ethereum.AETHC_Token_evt_Transfer
            WHERE "from" = 0x0000000000000000000000000000000000000000
            UNION ALL
            SELECT date_trunc('day', evt_block_time) as time, -(value * 1e-18) as amount
            FROM ankr_ethereum.AETHC_Token_evt_Transfer
            WHERE "to" = 0x0000000000000000000000000000000000000000
        ) as ankreth_flow
        right join (
            SELECT time
            FROM unnest(sequence(
                timestamp'2020-11-08 00:00:00', 
                CAST(NOW() as timestamp),
                interval '1' day)
            ) as s(time)
        ) as date_series
        ON ankreth_flow.time = date_series.time
        GROUP BY 1
    )

    select time,
      evt_tx_hash AS hash,
      wallet,
      NULL AS counterparty,
      0xA35b1B31Ce002FBF2058D22F30f95D405200A15b AS contract_address,
      TRY_CAST(NULL AS DOUBLE) AS qty
      , TRY_CAST(NULL AS DOUBLE) AS eth_qty
      , (supply/(1e-18 * ratio.newRatio))/supply as rate
    FROM ankreth_supply JOIN ankr_ethereum.AETHC_Token_evt_RatioUpdate as ratio on ankreth_supply.time = date_trunc('day', ratio.evt_block_time)
  CROSS JOIN ankreth_wallets

  ),
  ankreth_unioned AS (
    SELECT
      *,
      TRY_CAST(NULL AS DOUBLE) AS rate
    FROM
      ankreth_tokenflows
    UNION ALL
    SELECT
      *
    FROM
      ankreth_rates
  ),
  ankreth_info AS (
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
      ) AS ankreth_rebase_accrual,
      (cum_qty) * rate_filled AS ankreth_balance
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
              ankreth_unioned
          ) AS sub
      ) AS sub2
    WHERE cum_qty IS NOT NULL
  ),
  ankreth_info_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,/* treating forms of ankreth as weth */ 
      equivalent_qty / 1e18 AS qty, --equivalent eth amount of deposits/withdrawals into ankreth
      eth_qty / 1e18 AS eth_qty, --actual sum deposited which seemingly includes a ~5 bps fee
      ankreth_rebase_accrual / 1e18 AS ankreth_rebase_accrual,
      price
    FROM
      ankreth_info
      JOIN ankreth_prices ON CAST(ts AS DATE) = period
  ),
  ankreth_balance AS (
    SELECT
      CAST(ts AS DATE) AS period,
      wallet,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      /* fungible with weth */ ankreth_balance / 1e18 AS qty
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
          ankreth_info
      ) AS sub
    WHERE
      rn = 1
  ),
  ankreth_details_1 AS (
    SELECT
      period,
      wallet,
      contract_address,
      qty,
      price AS usd_price,
      qty * price AS usd_value
    FROM
      ankreth_balance
      INNER JOIN ankreth_prices USING (period, contract_address)
    WHERE
      qty > 0
  ),
  ankreth_details_lag AS (
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
      ankreth_details_1
  ),
  ankreth_accounting AS (
    SELECT
      'M2M-ANKRETH-' || CAST(RANK() OVER (
        ORDER BY
          period,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131008 AS account,
      'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ankreth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-ANKRETH-' || CAST(RANK() OVER (
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
      ankreth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    /* Below we grab mints (/burns) of steth from eth and move them between the steth and eth accounts. swaps are taken care of in the swaps queries, transfers in the transfers query. */
    SELECT
      'ENTER-EXIT-ANKRETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131008 AS account,
      'P&L' AS ledger,
      ts,
      qty * price AS amount,
      contract_address AS token_address,
      qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ankreth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL
    UNION ALL
    SELECT
      'ENTER-EXIT-ANKRETH-' || CAST(RANK() OVER (
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
      ankreth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND NOT qty IS NULL /* excluding wsteth mints/burns */
    UNION ALL
    /* Below we grab instant pnl of mints (mint fees)*/
    SELECT
      'INSTPNL-ANKRETH-' || CAST(RANK() OVER (
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
      ankreth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND eth_qty IS NOT NULL
    UNION ALL
    SELECT
      'INSTPNL-ANKRETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321011 AS account,
      'P&L' AS ledger,
      ts,
      (qty - eth_qty) * price AS amount,
      contract_address AS token_address,
      qty - eth_qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ankreth_info_2
    WHERE
      counterparty = 0x0000000000000000000000000000000000000000
      AND eth_qty IS NOT NULL
    UNION ALL
    SELECT
      'ACCRUAL-ANKRETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131008 AS account,
      'P&L' AS ledger,
      ts,
      ankreth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      ankreth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ankreth_info_2
    WHERE
      ankreth_rebase_accrual > 0
    UNION ALL
    SELECT
      'ACCRUAL-ANKRETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321011 AS account,
      'P&L' AS ledger,
      ts,
      ankreth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      ankreth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      ankreth_info_2
    WHERE
      ankreth_rebase_accrual > 0
  )
SELECT
  *
FROM
  ankreth_accountingg