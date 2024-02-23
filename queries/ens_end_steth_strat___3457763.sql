-- part of a query repo
-- query name: ENS-EnDAOment-stETH-Strategy
-- query link: https://dune.com/queries/3457763


WITH
  steth_wallets AS (
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet,
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
  steth_prices AS (
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
  steth_tokenflows AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "to" AS counterparty,
      contract_address,
      - CAST(value AS DOUBLE) AS qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN steth_wallets ON "from" = wallet
    WHERE
      contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
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
      INNER JOIN steth_wallets ON "to" = wallet
    WHERE
      contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
  ),
  steth_wtokenflows AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      "to" AS counterparty,
      contract_address,
      - CAST(value AS DOUBLE) AS qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN steth_wallets ON "from" = wallet
    WHERE
      contract_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0
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
      INNER JOIN steth_wallets ON "to" = wallet
    WHERE
      contract_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0
  ),
  oracle as (
    
    SELECT evt_block_time, evt_tx_hash, postTotalPooledEther * 1e0 / totalShares as rate
    FROM lido_ethereum.LegacyOracle_evt_PostTotalShares
    WHERE evt_block_time <= DATE'2023-05-15'

    union all
    
    SELECT
        evt_block_time, evt_tx_hash, (postTotalEther * 1e0) / postTotalShares AS rate
    FROM
      lido_ethereum.steth_evt_TokenRebased
  )
  , steth_wtokenflows_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      contract_address,
      NULL AS qty,
      qty * rate AS wqty,
      rate/* storing equivalent steth quantity of wsteth in its own column as otherwise sometimes the cumulative sums introduces small rounding errors */
    FROM
      (
        SELECT
          fl.*,
          rate,
          ROW_NUMBER() OVER (
            PARTITION BY
              fl.ts
            ORDER BY
              rt.evt_block_time DESC
          ) AS rn
        FROM
          steth_wtokenflows AS fl
          LEFT JOIN oracle AS rt ON fl.ts >= rt.evt_block_time
      )
    WHERE
      rn = 1
  ),
  steth_rebase_rates AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      wallet,
      NULL AS counterparty,
      0xae7ab96520de3a18e5e111b5eaab095312d7fe84 AS contract_address,
      NULL AS qty,
      NULL AS wqty,
      rate
    FROM
      oracle
      CROSS JOIN steth_wallets
  ),
  steth_unioned AS (
    SELECT
      *,
      NULL AS wqty,
      NULL AS rate
    FROM
      steth_tokenflows
    UNION ALL
    SELECT
      *
    FROM
      steth_wtokenflows_2
    UNION ALL
    SELECT
      *
    FROM
      steth_rebase_rates
  ),
  steth_info AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      contract_address,
      qty,
      wqty,
      (cum_normalized_qty + cum_normalized_wqty) * (
        rate_filled - LAG(rate_filled) OVER (
          PARTITION BY
            wallet
          ORDER BY
            ts
        )
      ) AS steth_rebase_accrual,
      (cum_normalized_qty + cum_normalized_wqty) * rate_filled AS steth_balance
    FROM
      (
        SELECT
          *,
          qty / rate_filled AS normalized_qty,
          SUM(qty / rate_filled) OVER (
            PARTITION BY
              wallet
            ORDER BY
              ts
          ) AS cum_normalized_qty,
          SUM(wqty / rate_filled) OVER (
            PARTITION BY
              wallet
            ORDER BY
              ts
          ) AS cum_normalized_wqty
        FROM
          (
            SELECT
              *,
              MAX(rate) OVER (
                PARTITION BY
                  rate_grp
              ) AS rate_filled
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
                  steth_unioned
              ) AS sub
          ) AS sub2
      ) AS sub3
    WHERE
      NOT cum_normalized_qty IS NULL
  ),
  steth_info_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,/* treating forms of steth as weth */ 
      qty / 1e18 AS qty,/* just steth, this field is used only for mints/burns of steth */ 
      steth_rebase_accrual / 1e18 AS steth_rebase_accrual,
      price
    FROM
      steth_info
      JOIN steth_prices ON CAST(ts AS DATE) = period
  ),
  steth_balance AS (
    SELECT
      CAST(ts AS DATE) AS period,
      wallet,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      /* fungible with weth */ steth_balance / 1e18 AS qty
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
          steth_info
      ) AS sub
    WHERE
      rn = 1
  ),
  steth_details_1 AS (
    SELECT
      period,
      wallet,
      contract_address,
      qty,
      price AS usd_price,
      qty * price AS usd_value
    FROM
      steth_balance
      INNER JOIN steth_prices USING (period, contract_address)
    WHERE
      qty > 0
  ),
  steth_details_lag AS (
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
      steth_details_1
  ),
  steth_accounting AS (
    SELECT
      'M2M-STETH-' || CAST(RANK() OVER (
        ORDER BY
          period,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131004 AS account,
      'M2M' AS ledger,
      period,
      lag_qty * (usd_price - lag_price) AS amount,
      contract_address AS token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_qty AS VARCHAR) || ' price ' || CAST(usd_price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      steth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-STETH-' || CAST(RANK() OVER (
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
      steth_details_lag
    WHERE
      COALESCE(lag_qty * (usd_price - lag_price), 0) <> 0
    UNION ALL
    /* Below we grab mints (/burns) of steth from eth and move them between the steth and eth accounts. swaps are taken care of in the swaps queries, transfers in the transfers query. */
    SELECT
      'ENTER-EXIT-STETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131004 AS account,
      'P&L' AS ledger,
      ts,
      qty * price AS amount,
      contract_address AS token_address,
      qty AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      steth_info_2
    WHERE
      counterparty IN (0x0000000000000000000000000000000000000000, 0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1) --lido withdrawal queue. this effectively puts eth back into the 'eth' account as soon as stETH hits the withdrawal queue contract
      AND NOT qty IS NULL /* excluding wsteth mints/burns */
    UNION ALL
    SELECT
      'ENTER-EXIT-STETH-' || CAST(RANK() OVER (
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
      steth_info_2
    WHERE
      counterparty IN (0x0000000000000000000000000000000000000000, 0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1) --lido withdrawal queue. this effectively puts eth back into the 'eth' account as soon as stETH hits the withdrawal queue contract
      AND NOT qty IS NULL /* excluding wsteth mints/burns */
    UNION ALL
    SELECT
      'ACCRUAL-STETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131004 AS account,
      'P&L' AS ledger,
      ts,
      steth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      steth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      steth_info_2
    WHERE
      steth_rebase_accrual > 0
    UNION ALL
    SELECT
      'ACCRUAL-STETH-' || CAST(RANK() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321006 AS account,
      'P&L' AS ledger,
      ts,
      steth_rebase_accrual * price AS amount,
      contract_address AS token_address,
      steth_rebase_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      steth_info_2
    WHERE
      steth_rebase_accrual > 0

  )
SELECT
  *
FROM
  steth_accountinge_accrual > 0

  )
SELECT
  *
FROM
  steth_accounting