-- part of a query repo
-- query name: ENS-EnDAOment-Curve-STETH-Strategy
-- query link: https://dune.com/queries/3457717


WITH
  curvesteth_wallet_list AS (
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet
  ),
  curvesteth_prices AS (
    SELECT
      DATE_TRUNC('DAY', minute) AS period,
      contract_address,
      price
    FROM
      prices.usd
      /* JOIN tokens USING (contract_address) */
    WHERE
      blockchain = 'ethereum'
      AND EXTRACT(
        HOUR
        FROM
          minute
      ) = 23
      AND EXTRACT(
        MINUTE
        FROM
          minute
      ) = 59
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      AND CAST(minute AS DATE) > CAST('2019-01-01' AS TIMESTAMP)
  ),
  curvesteth_curve_deposits_withdrawals_1 AS (
    SELECT
      evt.evt_block_time AS ts,
      evt.evt_tx_hash AS hash,
      evt.provider AS wallet,
      call.output_0 / 1e18 AS curve_deposit_amount,
      (
        CAST(ELEMENT_AT(token_amounts, 1) AS DOUBLE) + CAST(ELEMENT_AT(token_amounts, 2) AS DOUBLE)
      ) / 1e18 AS weth_deposit_amount_combined,
      CAST(ELEMENT_AT(token_amounts, 1) AS DOUBLE) / 1e18 AS weth_deposit_amount_weth,
      CAST(ELEMENT_AT(token_amounts, 2) AS DOUBLE) / 1e18 AS weth_deposit_amount_steth
    FROM
      curvefi_ethereum.steth_swap_evt_AddLiquidity AS evt
      JOIN curvefi_ethereum.steth_swap_call_add_liquidity AS call ON evt.evt_tx_hash = call.call_tx_hash
      AND evt.token_amounts = call.amounts
      JOIN curvesteth_wallet_list ON evt.provider = curvesteth_wallet_list.wallet
    WHERE
      call.call_success
    UNION ALL
    SELECT
      evt.evt_block_time AS ts,
      evt.evt_tx_hash AS hash,
      evt.provider AS wallet,
      - CAST(_amount AS DOUBLE) / 1e18 AS curve_deposit_amount,
      - (
        CAST(ELEMENT_AT(token_amounts, 1) AS DOUBLE) + CAST(ELEMENT_AT(token_amounts, 2) AS DOUBLE)
      ) / 1e18 AS weth_deposit_amount_combined,
      - CAST(ELEMENT_AT(token_amounts, 1) AS DOUBLE) / 1e18 AS weth_deposit_amount_weth,
      - CAST(ELEMENT_AT(token_amounts, 2) AS DOUBLE) / 1e18 AS weth_deposit_amount_steth
    FROM
      curvefi_ethereum.steth_swap_evt_RemoveLiquidity AS evt
      JOIN curvefi_ethereum.steth_swap_call_remove_liquidity AS call ON evt.evt_tx_hash = call.call_tx_hash
      AND evt.token_amounts = call.output_0 /* this could result in dupes if a single transaction deposits the exact same token amounts more than once but shouldn't be an issue in the case of the endaoment's sole wallet */
      JOIN curvesteth_wallet_list ON evt.provider = curvesteth_wallet_list.wallet
    WHERE
      call.call_success
      AND CAST(_amount AS DOUBLE) + 0 <> 0 /* reduce clutter */
    UNION ALL
    SELECT
      evt.evt_block_time AS ts,
      evt.evt_tx_hash AS hash,
      evt.provider AS wallet,
      - CAST(output_0 AS DOUBLE) / 1e18 AS curve_deposit_amount,
      - (
        CAST(ELEMENT_AT(token_amounts, 1) AS DOUBLE) + CAST(ELEMENT_AT(token_amounts, 2) AS DOUBLE)
      ) / 1e18 AS weth_deposit_amount_combined,
      - CAST(ELEMENT_AT(token_amounts, 1) AS DOUBLE) / 1e18 AS weth_deposit_amount_weth,
      - CAST(ELEMENT_AT(token_amounts, 2) AS DOUBLE) / 1e18 AS weth_deposit_amount_steth
    FROM
      curvefi_ethereum.steth_swap_evt_RemoveLiquidityImbalance AS evt
      JOIN curvefi_ethereum.steth_swap_call_remove_liquidity_imbalance AS call ON evt.evt_tx_hash = call.call_tx_hash
      AND evt.token_amounts = call._amounts
      JOIN curvesteth_wallet_list ON evt.provider = curvesteth_wallet_list.wallet
    WHERE
      call.call_success
    UNION ALL
    SELECT
      evt.evt_block_time AS ts,
      evt.evt_tx_hash AS hash,
      evt.provider AS wallet,
      - CAST(_token_amount AS DOUBLE) / 1e18 AS curve_deposit_amount,
      - CAST(output_0 AS DOUBLE) / 1e18 AS weth_deposit_amount_combined,
      - CAST(output_0 AS DOUBLE) * (1-i) / 1e18 AS weth_deposit_amount_weth,
      - CAST(output_0 AS DOUBLE) * i / 1e18 AS weth_deposit_amount_steth
    FROM curvefi_ethereum.steth_swap_evt_RemoveLiquidityOne AS evt
    JOIN curvefi_ethereum.steth_swap_call_remove_liquidity_one_coin AS call ON evt.evt_tx_hash = call.call_tx_hash
      AND evt.token_amount = call._token_amount
      AND evt.coin_amount = call.output_0
    JOIN curvesteth_wallet_list ON evt.provider = curvesteth_wallet_list.wallet
    WHERE
      call.call_success
  ),
  curvesteth_curve_deposits_withdrawals_2 AS (
    SELECT
      ts,
      hash,
      wallet,
      CAST(curve_deposit_amount AS DOUBLE) AS curve_deposit_amount,
      CAST(weth_deposit_amount_combined AS DOUBLE) AS weth_deposit_amount_combined,
      CAST(weth_deposit_amount_weth AS DOUBLE) AS weth_deposit_amount_weth,
      CAST(weth_deposit_amount_steth AS DOUBLE) AS weth_deposit_amount_steth,
      rate
    FROM
      (
        SELECT
          curve.*,
          rate,
          ROW_NUMBER() OVER (
            PARTITION BY
              curve.hash
            ORDER BY
              rates.call_block_time DESC,
              rates.rate DESC
          ) AS rn
        FROM
          curvesteth_curve_deposits_withdrawals_1 AS curve
          LEFT JOIN (
            SELECT
              call_block_time,
              output_0 / 1e18 AS rate
            FROM
              curvefi_ethereum.steth_swap_call_get_virtual_price
            WHERE
              call_success
          ) AS rates ON curve.ts >= rates.call_block_time
      )
    WHERE
      rn = 1
  ),
  curvesteth_min_ts AS (
    SELECT
      wallet,
      MIN(ts) AS min_ts
    FROM
      curvesteth_curve_deposits_withdrawals_2
    GROUP BY
      1
  ),
  curvesteth_rates AS (
    SELECT
      call_block_time AS ts,
      call_tx_hash AS hash,
      wallet,
      TRY_CAST(NULL AS DECIMAL) AS curve_deposit_amount,
      TRY_CAST(NULL AS DECIMAL) AS weth_deposit_amount_combined,
      TRY_CAST(NULL AS DECIMAL) AS weth_deposit_amount_weth,
      TRY_CAST(NULL AS DECIMAL) AS weth_deposit_amount_steth,
      MAX(output_0) / 1e18 AS rate /* some transactions might call get_virtual_price multiple times, highest is latest */
    FROM
      curvefi_ethereum.steth_swap_call_get_virtual_price
      JOIN curvesteth_min_ts ON call_block_time >= min_ts
    WHERE
      call_success
    GROUP BY
      1,
      2,
      3
  ),
  curvesteth_unioned AS (
    SELECT
      *
    FROM
      curvesteth_curve_deposits_withdrawals_2
    UNION ALL
    SELECT
      *
    FROM
      curvesteth_rates
    UNION ALL
    /* Filler */
    SELECT
      ts,
      NULL AS hash,
      wallet,
      TRY_CAST(NULL AS DECIMAL) AS curve_deposit_amount,
      TRY_CAST(NULL AS DECIMAL) AS weth_deposit_amount_combined,
      TRY_CAST(NULL AS DECIMAL) AS weth_deposit_amount_weth,
      TRY_CAST(NULL AS DECIMAL) AS weth_deposit_amount_steth,
      TRY_CAST(NULL AS DECIMAL) AS rate
    FROM
      curvesteth_min_ts
      JOIN (
        SELECT
          ts
        FROM
          UNNEST (
            SEQUENCE(
              CAST(
                SUBSTR(CAST('2019-01-01' AS VARCHAR), 1, 10) AS DATE
              ),
              CURRENT_DATE + INTERVAL '1' DAY,
              INTERVAL '1' DAY
            ) /* WARNING: Check out the docs for example of time series generation: https://dune.com/docs/query/syntax-differences/ */
          ) AS _u (ts)
      ) ON ts > CAST(min_ts AS DATE)
  ),
  curvesteth_cum_sum AS (
    SELECT
      CASE
        WHEN hash IS NULL THEN ts - INTERVAL '1' SECOND
        ELSE ts
      END AS ts,
      hash,
      wallet,
      CAST(curve_deposit_amount AS DOUBLE) AS curve_deposit_amount,
      CAST(weth_deposit_amount_combined AS DOUBLE) AS weth_deposit_amount_combined,
      CAST(weth_deposit_amount_weth AS DOUBLE) AS weth_deposit_amount_weth,
      CAST(weth_deposit_amount_steth AS DOUBLE) AS weth_deposit_amount_steth,
      rate,
      CAST(curve_deposit_amount AS DOUBLE) * rate - CAST(weth_deposit_amount_combined AS DOUBLE) AS instant_weth_pnl,
      SUM(CAST(curve_deposit_amount AS DOUBLE)) OVER (
        ORDER BY
          ts,
          rate
      ) AS cumulative_curve_tok,
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
              ts,
              rate
          ) AS rate_grp
        FROM
          curvesteth_unioned
      )
  ),
  curvesteth_with_accruals AS (
    SELECT
      ts,
      hash,
      wallet,
      CAST(weth_deposit_amount_weth AS DOUBLE) AS weth_deposit_amount_weth,
      CAST(weth_deposit_amount_steth AS DOUBLE) AS weth_deposit_amount_steth,
      rate_filled AS rate,
      instant_weth_pnl,
      cumulative_curve_tok,
      cumulative_curve_tok * (
        rate - LAG(rate_filled) OVER (
          ORDER BY
            ts,
            rate_filled
        )
      ) AS weth_accrual,
      price,
      contract_address AS token_address,
      cumulative_curve_tok * rate_filled AS balance
    FROM
      curvesteth_cum_sum
      LEFT JOIN curvesteth_prices ON CAST(ts AS DATE) = period /* only weth in the prices table */
  ),
  curvesteth_accounting AS (
    SELECT
      'DEPWITH-CURVE_STETH-WETH-' || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          CAST(weth_deposit_amount_weth AS DOUBLE)
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131002 AS account,
      /* increase in aura weth asset */ 'P&L' AS ledger,
      ts,
      CAST(weth_deposit_amount_weth AS DOUBLE) * price AS amount,
      token_address,
      CAST(weth_deposit_amount_weth AS DOUBLE) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(CAST(weth_deposit_amount_weth AS DOUBLE), 0) <> 0
    UNION ALL
    SELECT
      'DEPWITH-CURVE_STETH-WETH-' || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          CAST(weth_deposit_amount_weth AS DOUBLE)
      ) AS VARCHAR) AS transaction,
      2 AS line,
      121 AS account,
      /* decrease in eth asset */ 'P&L' AS ledger,
      ts,
      - CAST(weth_deposit_amount_weth AS DOUBLE) * price AS amount,
      token_address,
      - CAST(weth_deposit_amount_weth AS DOUBLE) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(CAST(weth_deposit_amount_weth AS DOUBLE), 0) <> 0
    UNION ALL
    SELECT
      'DEPWITH-CURVE_STETH-STETH-' || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          CAST(weth_deposit_amount_steth AS DOUBLE)
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131002 AS account,
      /* increase in aura weth asset */ 'P&L' AS ledger,
      ts,
      CAST(weth_deposit_amount_steth AS DOUBLE) * price AS amount,
      token_address,
      CAST(weth_deposit_amount_steth AS DOUBLE) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(CAST(weth_deposit_amount_steth AS DOUBLE), 0) <> 0
    UNION ALL
    SELECT
      'DEPWITH-CURVE_STETH-STETH-' || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          CAST(weth_deposit_amount_steth AS DOUBLE)
      ) AS VARCHAR) AS transaction,
      2 AS line,
      131004 AS account,
      /* decrease in steth asset */ 'P&L' AS ledger,
      ts,
      - CAST(weth_deposit_amount_steth AS DOUBLE) * price AS amount,
      token_address,
      - CAST(weth_deposit_amount_steth AS DOUBLE) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(CAST(weth_deposit_amount_steth AS DOUBLE), 0) <> 0
    UNION ALL
    SELECT
      CONCAT('INSTPNL-CURVE_STETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          instant_weth_pnl
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131002 AS account,
      'P&L' AS ledger,
      ts,
      instant_weth_pnl * price AS amount,
      token_address,
      instant_weth_pnl AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(instant_weth_pnl, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('INSTPNL-CURVE_STETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          instant_weth_pnl
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321004 AS account,
      /* this pnl stems from depositing into or withdrawing from curve so it applies to curve steth lp pnl account */ 'P&L' AS ledger,
      ts,
      instant_weth_pnl * price AS amount,
      token_address,
      instant_weth_pnl AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(instant_weth_pnl, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('ACCRUAL-CURVE_STETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          rate
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131002 AS account,
      /* increase in curve steth lp asset */ 'P&L' AS ledger,
      ts,
      weth_accrual * price AS amount,
      token_address,
      weth_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(weth_accrual, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('ACCRUAL-CURVE_STETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet,
          rate
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321004 AS account,
      /* increase in curve steth lp pnl */ 'P&L' AS ledger,
      ts,
      weth_accrual * price AS amount,
      token_address,
      weth_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      curvesteth_with_accruals
    WHERE
      COALESCE(weth_accrual, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('M2M-CURVE_STETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131002 AS account,
      'M2M' AS ledger,
      ts,
      lag_balance * (price - lag_price) AS amount,
      token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_balance AS VARCHAR) || ' price ' || CAST(price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      (
        SELECT
          *,
          LAG(price) OVER (
            ORDER BY
              ts,
              rate
          ) AS lag_price,
          LAG(balance) OVER (
            ORDER BY
              ts,
              rate
          ) AS lag_balance
        FROM
          curvesteth_with_accruals
        WHERE
          hash IS NULL
      ) AS sub
    WHERE
      COALESCE(lag_balance * (price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      CONCAT('M2M-CURVE_STETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32311 AS account,
      'M2M' AS ledger,
      ts,
      lag_balance * (price - lag_price) AS amount,
      token_address,
      0 AS token_amount,
      'qty ' || CAST(lag_balance AS VARCHAR) || ' price ' || CAST(price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      (
        SELECT
          *,
          LAG(price) OVER (
            ORDER BY
              ts,
              rate
          ) AS lag_price,
          LAG(balance) OVER (
            ORDER BY
              ts,
              rate
          ) AS lag_balance
        FROM
          curvesteth_with_accruals
        WHERE
          hash IS NULL
      ) AS sub
    WHERE
      COALESCE(lag_balance * (price - lag_price), 0) <> 0
  )
SELECT
  *
FROM
  curvesteth_accounting
/* crv, ldo rewards taken care of in swaps */ounting
/* crv, ldo rewards taken care of in swaps */