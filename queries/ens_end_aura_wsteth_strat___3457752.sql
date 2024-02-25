-- part of a query repo
-- query name: ENS-EnDAOment-Aura-WSTETH-Strategy
-- query link: https://dune.com/queries/3457752


WITH
  aurawsteth_wallet_list AS (
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet
  ),
  aurawsteth_prices AS (
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
  aurawsteth_aura_deposits_withdrawals_1 AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      owner AS wallet,
      CAST(assets AS DOUBLE) / 1e18 AS aura_deposit_amount /* also same as balancer deposit amount */
    FROM
      aura_finance_ethereum.BaseRewardPool4626_evt_Deposit
      JOIN aurawsteth_wallet_list ON owner = wallet
    WHERE
      contract_address = 0xe4683fe8f53da14ca5dac4251eadfb3aa614d528
    UNION ALL
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      owner AS wallet,
      - CAST(assets AS DOUBLE)/ 1e18 AS aura_deposit_amount /* also same as balancer deposit amount */
    FROM
      aura_finance_ethereum.BaseRewardPool4626_evt_Withdraw
      JOIN aurawsteth_wallet_list ON owner = wallet
    WHERE
      contract_address = 0xe4683fe8f53da14ca5dac4251eadfb3aa614d528
      
    UNION ALL
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS hash,
      user AS wallet,
      - CAST(amount AS DOUBLE)/ 1e18 AS aura_deposit_amount /* also same as balancer deposit amount */
    FROM
      aura_finance_ethereum.BaseRewardPool4626_evt_Withdrawn
      JOIN aurawsteth_wallet_list ON user = wallet
    WHERE
      contract_address = 0xe4683fe8f53da14ca5dac4251eadfb3aa614d528
  ),
  aurawsteth_aura_deposits_withdrawals_2 AS (
    SELECT
      COALESCE(a.ts, b.ts) AS ts,
      COALESCE(a.hash, b.hash) AS hash,
      COALESCE(a.wallet, b.wallet) AS wallet,
      COALESCE(a.aura_deposit_amount, b.aura_deposit_amount) AS aura_deposit_amount,
      CAST(ELEMENT_AT(output_amountsIn, 2) AS DOUBLE) / 1e18 AS weth_deposit_amount,
      CAST(ELEMENT_AT(output_amountsIn, 1) AS DOUBLE) / 1e18 AS wsteth_deposit_amount
    FROM
      balancer_v2_ethereum.MetaStablePool_call_onJoinPool joinp
      LEFT JOIN aurawsteth_aura_deposits_withdrawals_1 AS a ON joinp.call_tx_hash = a.hash
      LEFT JOIN balancer_v2_ethereum.MetaStablePool_evt_Transfer xfer
      ON joinp.contract_address = xfer.contract_address
      AND joinp.recipient = xfer.to
      AND xfer."from" = 0x0000000000000000000000000000000000000000
      AND joinp.call_tx_hash = xfer.evt_tx_hash
      LEFT JOIN aurawsteth_aura_deposits_withdrawals_1 b
      ON b.wallet = xfer.to
      AND b.aura_deposit_amount = CAST(xfer.value AS DOUBLE) / 1e18
      AND joinp.call_tx_hash <> b.hash
    WHERE call_success
      AND (a.hash IS NOT NULL OR b.hash IS NOT NULL)
    UNION ALL
    SELECT
      COALESCE(a.ts, b.ts) AS ts,
      COALESCE(a.hash, b.hash) AS hash,
      COALESCE(a.wallet, b.wallet) AS wallet,
      COALESCE(a.aura_deposit_amount, b.aura_deposit_amount) AS aura_deposit_amount,
      - CAST(ELEMENT_AT(output_amountsOut, 2) AS DOUBLE) / 1e18 AS weth_deposit_amount,
      - CAST(ELEMENT_AT(output_amountsOut, 1) AS DOUBLE) / 1e18 AS wsteth_deposit_amount
    FROM
      balancer_v2_ethereum.MetaStablePool_call_onExitPool AS exit
      LEFT JOIN aurawsteth_aura_deposits_withdrawals_1 AS a ON exit.call_tx_hash = a.hash
      LEFT JOIN balancer_v2_ethereum.MetaStablePool_evt_Transfer xfer
      ON exit.contract_address = xfer.contract_address
      AND exit.sender = xfer."from"
      AND xfer.to = 0x0000000000000000000000000000000000000000
      AND exit.call_tx_hash = xfer.evt_tx_hash
      LEFT JOIN aurawsteth_aura_deposits_withdrawals_1 b
      ON b.wallet = xfer."from"
      AND b.aura_deposit_amount = - CAST(xfer.value AS DOUBLE) / 1e18
      AND exit.call_tx_hash <> b.hash
    WHERE call_success
      AND (a.hash IS NOT NULL OR b.hash IS NOT NULL)
  ),
  oracle as (
    
    SELECT evt_block_time, evt_tx_hash, postTotalPooledEther * 1e0 / totalShares as wsteth_rate
    FROM lido_ethereum.LegacyOracle_evt_PostTotalShares
    WHERE evt_block_time <= DATE'2023-05-15'

    union all
    
    SELECT
        evt_block_time, evt_tx_hash, (postTotalEther * 1e0) / postTotalShares AS wsteth_rate
    FROM
      lido_ethereum.steth_evt_TokenRebased
  )
  , aurawsteth_aura_deposits_withdrawals_3 AS (
    SELECT
      ts,
      hash,
      wallet,
      aura_deposit_amount,
      weth_deposit_amount,
      wsteth_deposit_amount,
      rate,
      wsteth_rate
    FROM
      (
        SELECT
          aura.*,
          rate,
          wsteth_rate,
          ROW_NUMBER() OVER (
            PARTITION BY
              aura.hash
            ORDER BY
              rates.call_block_time DESC,
              wsteth_rt.evt_block_time DESC,
              rates.rate DESC
          ) AS rn
        FROM
          aurawsteth_aura_deposits_withdrawals_2 AS aura
          LEFT JOIN (
            SELECT
              call_block_time,
              output_0 / 1e18 AS rate
            FROM
              balancer_v2_ethereum.MetaStablePool_call_getRate
            WHERE
              call_success
              AND contract_address = 0x32296969ef14eb0c6d29669c550d4a0449130230
          ) AS rates ON aura.ts >= rates.call_block_time
          LEFT JOIN oracle AS wsteth_rt ON aura.ts >= wsteth_rt.evt_block_time
      )
    WHERE
      rn = 1
  ),
  aurawsteth_min_ts AS (
    SELECT
      wallet,
      MIN(ts) AS min_ts
    FROM
      aurawsteth_aura_deposits_withdrawals_3
    GROUP BY
      1
  ),
  aurawsteth_rates AS (
    SELECT ts, hash, wallet, aura_deposit_amount, weth_deposit_amount, wsteth_deposit_amount, rate, wsteth_rate
    FROM
    (
        SELECT
          call_block_time AS ts,
          call_tx_hash AS hash,
          wallet,
          NULL AS aura_deposit_amount,
          NULL AS weth_deposit_amount,
          NULL AS wsteth_deposit_amount,
          output_0 / 1e18 AS rate,
          NULL AS wsteth_rate,
          ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_trace_address DESC) AS rn --to grab latest within trxn
        FROM
          balancer_v2_ethereum.MetaStablePool_call_getRate
          JOIN aurawsteth_min_ts ON call_block_time >= min_ts
        WHERE
          call_success
          AND contract_address = 0x32296969ef14eb0c6d29669c550d4a0449130230
    )
    WHERE rn = 1
  ),
  aurawsteth_unioned AS (
    SELECT
      *
    FROM
      aurawsteth_aura_deposits_withdrawals_3
    UNION ALL
    SELECT
      *
    FROM
      aurawsteth_rates
    UNION ALL
    /* Filler */
    SELECT
      ts,
      NULL AS hash,
      wallet,
      NULL AS aura_deposit_amount,
      NULL AS weth_deposit_amount,
      NULL AS wsteth_deposit_amount,
      NULL AS rate,
      NULL AS wsteth_rate
    FROM
      aurawsteth_min_ts
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
            )
          ) AS _u (ts)
      ) ON ts > CAST(min_ts AS DATE)
  ),
  aurawsteth_cum_sum AS (
    SELECT
      CASE
        WHEN hash IS NULL THEN ts - INTERVAL '1' SECOND
        ELSE ts
      END AS ts,
      hash,
      wallet,
      aura_deposit_amount,
      weth_deposit_amount,
      wsteth_deposit_amount,
      rate,
      wsteth_rate,
      aura_deposit_amount * rate - weth_deposit_amount - wsteth_deposit_amount * wsteth_rate AS instant_weth_pnl,
      SUM(CAST(aura_deposit_amount AS DOUBLE)) OVER (
        ORDER BY
          ts,
          rate
      ) AS cumulative_aura_tok,
      MAX(RATE) OVER (
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
          aurawsteth_unioned
      ) AS sub
  ),
  aurawsteth_with_accruals AS (
    SELECT
      ts,
      hash,
      wallet,
      weth_deposit_amount,
      wsteth_deposit_amount,
      rate_filled AS rate,
      wsteth_rate,
      instant_weth_pnl,
      cumulative_aura_tok,
      cumulative_aura_tok * (
        rate - LAG(rate_filled) OVER (
          ORDER BY
            ts,
            rate_filled
        )
      ) AS weth_accrual,
      price,
      contract_address AS token_address,
      cumulative_aura_tok * rate_filled AS balance
    FROM
      aurawsteth_cum_sum
      LEFT JOIN aurawsteth_prices ON CAST(ts AS DATE) = period /* only weth in the prices table */
  ),
  aurawsteth_accounting AS (
    SELECT
      CONCAT('DEPWITH-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          weth_deposit_amount + wsteth_deposit_amount * wsteth_rate
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131001 AS account,/* increase in aura wsteth asset */
      'P&L' AS ledger,
      ts,
      (weth_deposit_amount + wsteth_deposit_amount * wsteth_rate) * price AS amount,
      token_address,
      weth_deposit_amount + wsteth_deposit_amount * wsteth_rate AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(weth_deposit_amount + wsteth_deposit_amount * wsteth_rate, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('DEPWITH-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          weth_deposit_amount + wsteth_deposit_amount * wsteth_rate
      ) AS VARCHAR) AS transaction,
      2 AS line,
      121 AS account,/* decrease in eth asset */
      'P&L' AS ledger,
      ts,
      - CAST(weth_deposit_amount AS DOUBLE) * price AS amount,
      token_address,
      - CAST(weth_deposit_amount AS DOUBLE) AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(weth_deposit_amount + wsteth_deposit_amount * wsteth_rate, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('DEPWITH-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          weth_deposit_amount + wsteth_deposit_amount * wsteth_rate
      ) AS VARCHAR) AS transaction,
      3 AS line,
      131004 AS account,/* decrease in steth asset */
      'P&L' AS ledger,
      ts,
      - wsteth_deposit_amount * wsteth_rate * price AS amount,
      token_address,
      - wsteth_deposit_amount * wsteth_rate AS token_amount, --unwrapped steth equivalent
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(weth_deposit_amount + wsteth_deposit_amount * wsteth_rate, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('INSTPNL-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          instant_weth_pnl
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131001 AS account,
      'P&L' AS ledger,
      ts,
      instant_weth_pnl * price AS amount,
      token_address,
      instant_weth_pnl AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(instant_weth_pnl, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('INSTPNL-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          instant_weth_pnl
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321003 AS account,
      'P&L' AS ledger,
      ts,
      instant_weth_pnl * price AS amount,
      token_address,
      instant_weth_pnl AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(instant_weth_pnl, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('ACCRUAL-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          rate
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131001 AS account,/* increase in aura wsteth asset */
      'P&L' AS ledger,
      ts,
      weth_accrual * price AS amount,
      token_address,
      weth_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(weth_accrual, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('ACCRUAL-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          wallet
        ORDER BY
          ts,
          rate
      ) AS VARCHAR) AS transaction,
      2 AS line,
      32321003 AS account,/* increase in aura wsteth pnl */
      'P&L' AS ledger,
      ts,
      weth_accrual * price AS amount,
      token_address,
      weth_accrual AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      aurawsteth_with_accruals
    WHERE
      COALESCE(weth_accrual, 0) <> 0
    UNION ALL
    SELECT
      CONCAT('M2M-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          wallet
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131001 AS account,
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
          aurawsteth_with_accruals
        WHERE
          hash IS NULL
      ) AS sub
    WHERE
      COALESCE(lag_balance * (price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      CONCAT('M2M-AURA-WSTETH', '-') || CAST(ROW_NUMBER() OVER (
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
          aurawsteth_with_accruals
        WHERE
          hash IS NULL
      ) AS sub
    WHERE
      COALESCE(lag_balance * (price - lag_price), 0) <> 0
  )
SELECT
  *
FROM
  aurawsteth_accounting WHERE amount <> 0
  /* deposit/withdrawal --> weth_deposit_amount from main account to aura or vice versa, assets move (+1 -1) */
  /* instant pnl */
  /* equity also increases by this amount in a similar destination */
  /* accrual - assets in aura and equty increase by this amount */
  /* aura rewards taken care of in swaps query */ 
