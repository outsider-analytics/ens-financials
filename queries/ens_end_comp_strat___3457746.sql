-- part of a query repo
-- query name: ens-end-comp-strat
-- query link: https://dune.com/queries/3457746


WITH
  comp_wallet_list AS (
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet /* endaoment addy */
  ) /* UNION ALL */,
  comp_contract_address_list AS (
    SELECT
      0x5d3a536e4d6dbd6114cc1ead35777bab948e3643 AS contract_address,
      18 AS underlying_decimals,
      'DAI' AS token,
      0x6b175474e89094c44da98b954eedeac495271d0f AS token_address /* cDAI */
    UNION ALL
    SELECT
      0x39aa39c021dfbae8fac545936693ac917d5e7563 AS contract_address,
      6 AS underlying_decimals,
      'USDC' AS token,
      0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS token_address /* cUSDC */
  ),
  comp_unioned_supply AS (
    SELECT
      "from" AS wallet,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      - CAST(amount AS DOUBLE) AS delta_ctok,
      contract_address
    FROM
      compound_v2_ethereum.cErc20_evt_Transfer AS t
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      "from" <> contract_address
      AND CAST(amount AS DOUBLE) + 0 <> 0
    UNION ALL
    SELECT
      "to" AS wallet,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      CAST(amount AS DOUBLE) AS delta_ctok,
      contract_address
    FROM
      compound_v2_ethereum.cErc20_evt_Transfer AS t
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      "to" <> contract_address
      AND CAST(amount AS DOUBLE) + 0 <> 0
    UNION ALL
    SELECT
      "from" AS wallet,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      - CAST(amount AS DOUBLE) AS delta_ctok,
      contract_address
    FROM
      compound_v2_ethereum.CErc20Delegator_evt_Transfer AS t
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      "from" <> contract_address
      AND CAST(amount AS DOUBLE) + 0 <> 0
    UNION ALL
    SELECT
      "to" AS wallet,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      CAST(amount AS DOUBLE) AS delta_ctok,
      contract_address
    FROM
      compound_v2_ethereum.CErc20Delegator_evt_Transfer AS t
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      "to" <> contract_address
      AND CAST(amount AS DOUBLE) + 0 <> 0
  ),
  comp_rates_raw AS (
    SELECT
      contract_address,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      CAST(redeemAmount AS DOUBLE) AS underlying,
      redeemTokens AS ctokens,
      CAST(redeemAmount AS DOUBLE) / NULLIF(redeemTokens, CAST(0 AS UINT256)) AS exchange_rate
    FROM
      compound_v2_ethereum.cErc20_evt_Redeem AS r
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      CAST(redeemAmount AS DOUBLE) + 0 > 1000000000000 /* rounding error makes the rate for trxns with a low amount off -- this cutoff works well for the abvoe contract addy */
    UNION ALL
    SELECT
      contract_address,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      CAST(mintAmount AS DOUBLE) AS underlying,
      mintTokens AS ctokens,
      CAST(mintAmount AS DOUBLE) / NULLIF(mintTokens, CAST(0 AS UINT256)) AS exchange_rate
    FROM
      compound_v2_ethereum.cErc20_evt_Mint AS m
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      CAST(mintAmount AS DOUBLE) + 0 > 1000000000000 /* rounding error makes the rate for trxns with a low amount off -- this cutoff works well for the abvoe contract addy */
    UNION ALL
    SELECT
      contract_address,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      CAST(redeemAmount AS DOUBLE) AS underlying,
      redeemTokens AS ctokens,
      CAST(redeemAmount AS DOUBLE) / NULLIF(redeemTokens, CAST(0 AS UINT256)) AS exchange_rate
    FROM
      compound_v2_ethereum.CErc20Delegator_evt_Redeem AS r
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      CAST(redeemAmount AS DOUBLE) + 0 > 1000000000000 /* rounding error makes the rate for trxns with a low amount off -- this cutoff works well for the abvoe contract addy */
    UNION ALL
    SELECT
      contract_address,
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      CAST(mintAmount AS DOUBLE) AS underlying,
      mintTokens AS ctokens,
      CAST(mintAmount AS DOUBLE) / NULLIF(mintTokens, CAST(0 AS UINT256)) AS exchange_rate
    FROM
      compound_v2_ethereum.CErc20Delegator_evt_Mint AS m
      JOIN comp_contract_address_list USING (contract_address)
    WHERE
      CAST(mintAmount AS DOUBLE) + 0 > 1000000000000 /* rounding error makes the rate for trxns with a low amount off -- this cutoff works well for the abvoe contract addy */
  ),
  comp_rates_raw2 AS (
    SELECT
      contract_address,
      hash,
      underlying,
      ctokens,
      exchange_rate
    FROM
      comp_rates_raw
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  comp_rates_raw_excl AS (
    SELECT
      contract_address,
      hash,
      ctokens,
      COUNT(*) AS cnt
    FROM
      comp_rates_raw2
    GROUP BY
      1,
      2,
      3
    HAVING
      COUNT(*) > 1 /* need to exclude these (and caolesce aggregated rate from entire trxn instead) in order to avoid dupes */
  ),
  comp_rates_raw3 AS (
    SELECT
      comp_rates_raw2.*
      , contract_address
      , hash
      , ctokens
    FROM
      comp_rates_raw2
      LEFT JOIN comp_rates_raw_excl USING (contract_address, hash, ctokens)
    WHERE
      cnt IS NULL
  ),
  comp_rates_agg AS (
    SELECT
      contract_address,
      ts,
      hash,
      SUM(underlying) / NULLIF(SUM(ctokens), CAST(0 AS UINT256)) AS exchange_rate
    FROM
      comp_rates_raw
    GROUP BY
      1,
      2,
      3
    HAVING
      SUM(ctokens) <> CAST(0 AS UINT256)
  ),
  comp_with_rates_1 AS (
    SELECT
      comp_unioned_supply.*,
      COALESCE(
        comp_unioned_supply.delta_ctok / ABS(comp_unioned_supply.delta_ctok) * rates.underlying,
        comp_rates_agg.exchange_rate * comp_unioned_supply.delta_ctok
      ) AS delta_underlying,
      COALESCE(rates.exchange_rate, comp_rates_agg.exchange_rate) AS exchange_rate
    FROM
      comp_unioned_supply
      LEFT JOIN comp_rates_raw3 AS rates ON comp_unioned_supply.hash = rates.hash
      AND ABS(comp_unioned_supply.delta_ctok) = CAST(rates.ctokens AS DOUBLE)
      AND comp_unioned_supply.contract_address = rates.contract_address
      LEFT JOIN comp_rates_agg ON comp_unioned_supply.hash = comp_rates_agg.hash
      AND comp_unioned_supply.contract_address = comp_rates_agg.contract_address
  ),
  comp_with_rates_2 AS (
    SELECT
      *
    FROM
      comp_with_rates_1
    WHERE
      NOT exchange_rate IS NULL
    UNION ALL
    SELECT
      wallet,
      ts,
      hash,
      delta_ctok,
      contract_address,
      delta_ctok * exchange_rate_filled AS delta_underlying,
      exchange_rate_filled AS exchange_rate
    FROM
      (
        SELECT
          *,
          MAX(exchange_rate) OVER (
            PARTITION BY
              contract_address,
              rate_grp
          ) AS exchange_rate_filled
        FROM
          (
            SELECT
              *,
              SUM(
                CASE
                  WHEN NOT exchange_rate IS NULL THEN 1
                  ELSE 0
                END
              ) OVER (
                PARTITION BY
                  contract_address
                ORDER BY
                  ts NULLS FIRST,
                  COALESCE(exchange_rate, 99999999999999999) NULLS FIRST
              ) AS rate_grp
            FROM
              (
                SELECT
                  *
                FROM
                  comp_with_rates_1
                WHERE
                  exchange_rate IS NULL
                UNION ALL
                SELECT
                  NULL AS wallet,
                  ts,
                  hash,
                  TRY_CAST(NULL AS DECIMAL) AS delta_ctok,
                  contract_address,
                  TRY_CAST(NULL AS DECIMAL) AS delta_underlying,
                  exchange_rate
                FROM
                  comp_rates_agg
              ) AS sub
          ) AS sub2
      ) AS sub3
    WHERE
      exchange_rate IS NULL
  ),
  comp_with_rates AS (
    SELECT
      *
    FROM
      comp_with_rates_2
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
      TRY_CAST(NULL AS DECIMAL) AS delta_ctok,
      sub.contract_address,
      TRY_CAST(NULL AS DECIMAL) AS delta_underlying,
      exchange_rate
    FROM
      (
        SELECT
          contract_address,
          ts,
          hash,
          exchange_rate,
          ROW_NUMBER() OVER (
            PARTITION BY
              contract_address,
              CAST(ts AS DATE)
            ORDER BY
              ts DESC
          ) AS rn
        FROM
          comp_rates_agg
        LEFT JOIN (SELECT hash, 1 AS exclude FROM comp_with_rates) excl
        USING (hash)
        WHERE exclude IS NULL
      ) AS sub
      JOIN comp_min_ts ON ts >= min_ts
      AND sub.contract_address = comp_min_ts.contract_address
      AND rn = 1
  ),
  comp_unioned AS (
    SELECT
      wallet,
      ts,
      hash,
      delta_ctok,
      contract_address,
      delta_underlying,
      exchange_rate,
      cumulative_ctok,
      cumulative_underlying,
      exchange_rate - LAG(exchange_rate) OVER (
        PARTITION BY
          wallet,
          contract_address
        ORDER BY
          ts NULLS FIRST
      ) AS exchange_rate_increase,
      cumulative_ctok - COALESCE(delta_ctok, 0) AS previous_ctok,
      (cumulative_ctok - COALESCE(delta_ctok, 0)) * (
        exchange_rate - LAG(exchange_rate) OVER (
          PARTITION BY
            wallet,
            contract_address
          ORDER BY
            ts NULLS FIRST
        )
      ) AS interest_accrual
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
              *
            FROM
              comp_with_rates
            UNION ALL
            SELECT
              *
            FROM
              comp_rates
          ) AS sub
      ) AS sub2
  ),
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
        WHEN token = 'USDC' THEN 112001 /* increase in money market assets */
        WHEN token = 'DAI' THEN 112002
      END AS account,
      'P&L' AS ledger,
      ts,
      delta_underlying / POWER(10, underlying_decimals) AS amount,
      CASE
        WHEN token_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_address
      END AS token_address,
      delta_underlying / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list USING (contract_address)
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
        WHEN token = 'USDC' THEN 111001 /* decrease in cash assets */
        WHEN token = 'DAI' THEN 111002
      END AS account,
      'P&L' AS ledger,
      ts,
      - delta_underlying / POWER(10, underlying_decimals) AS amount,
      CASE
        WHEN token_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_address
      END AS token_address,
      - delta_underlying / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list USING (contract_address)
    WHERE
      NOT delta_underlying IS NULL
    UNION ALL
    SELECT
      'ACCRUAL-COMP-' || token || '-' || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          ts NULLS FIRST,
          interest_accrual NULLS FIRST
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token = 'USDC' THEN 112001 /* increase in money market assets */
        WHEN token = 'DAI' THEN 112002
      END AS account,
      'P&L' AS ledger,
      ts,
      interest_accrual / POWER(10, underlying_decimals) AS amount,
      CASE
        WHEN token_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_address
      END AS token_address,
      interest_accrual / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list USING (contract_address)
    WHERE
      COALESCE(interest_accrual, 0) <> 0
    UNION ALL
    SELECT
      'ACCRUAL-COMP-' || token || '-' || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          contract_address
        ORDER BY
          ts NULLS FIRST,
          interest_accrual NULLS FIRST
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token = 'USDC' THEN 32321001 /* increase in investment P&L */
        WHEN token = 'DAI' THEN 32321002
      END AS account,
      'P&L' AS ledger,
      ts,
      interest_accrual / POWER(10, underlying_decimals) AS amount,
      CASE
        WHEN token_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_address
      END AS token_address,
      interest_accrual / POWER(10, underlying_decimals) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      comp_unioned
      LEFT JOIN comp_contract_address_list USING (contract_address)
    WHERE
      COALESCE(interest_accrual, 0) <> 0
  )
SELECT
  *
FROM
  comp_accounting