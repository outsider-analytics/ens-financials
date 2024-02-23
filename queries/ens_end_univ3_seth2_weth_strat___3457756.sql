-- part of a query repo
-- query name: ENS-EnDAOment-UNIV3-SETH2-WETH-Strategy
-- query link: https://dune.com/queries/3457756


--Rounding makes some of the IL updates 0, reducing total row count vs spark. Everything still equals out perfectly in the end with just less (very small) updates to IL
--To make it all congruent, the entire table in snowflake will have to be replaced once we use dunesql sources.
WITH univ3seth2_wallet_list AS 
(
    SELECT
      0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet
), univ3seth2_prices AS 
(
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
), univ3seth2_univ3_nft_mints AS
(
  SELECT wallet
  , output_tokenId AS tokenId
  , json_query(params, 'lax $.token0' OMIT QUOTES) AS token0 --prob need to fix formatting
  , json_query(params, 'lax $.token1' OMIT QUOTES) AS token1
  , json_query(params, 'lax $.fee') AS fee
  , json_query(params, 'lax $.tickLower') AS tickLower
  , json_query(params, 'lax $.tickUpper') AS tickUpper
  , call_block_time AS mint_ts
  FROM uniswap_v3_ethereum.NonfungibleTokenPositionManager_call_mint mint
  JOIN univ3seth2_wallet_list
  ON json_query(mint.params, 'lax $.recipient' OMIT QUOTES) = CAST(univ3seth2_wallet_list.wallet AS VARCHAR)
  WHERE call_success
  AND json_query(params, 'lax $.token0' OMIT QUOTES) = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --this specific lp composition is hardcoded here and in other portions of the query, other portions will have to be restructured if we add more uni pools to this (might be easier to just make individual queries for each pool)
  AND json_query(params, 'lax $.token1' OMIT QUOTES) = '0xfe2e637202056d30016725477c5da089ab0a043a'
  AND json_query(params, 'lax $.fee') = '3000'
), univ3seth2_same_pool_nft_mints AS
(
  SELECT CASE WHEN output_tokenId = univ3seth2_univ3_nft_mints.tokenId THEN 1 ELSE 0 END AS is_owned
  , univ3seth2_univ3_nft_mints.wallet
  , output_tokenId AS tokenId
  , json_query(params, 'lax $.token0' OMIT QUOTES) AS token0 --prob need to fix formatting
  , json_query(params, 'lax $.token1' OMIT QUOTES) AS token1
  , json_query(params, 'lax $.fee') AS fee
  , json_query(params, 'lax $.tickLower') AS tickLower
  , json_query(params, 'lax $.tickUpper') AS tickUpper
  , mint_ts
  FROM uniswap_v3_ethereum.NonfungibleTokenPositionManager_call_mint mint
  JOIN univ3seth2_univ3_nft_mints
  ON json_query(params, 'lax $.token0' OMIT QUOTES) = univ3seth2_univ3_nft_mints.token0
  AND json_query(params, 'lax $.token1' OMIT QUOTES) = univ3seth2_univ3_nft_mints.token1
  AND json_query(params, 'lax $.fee') = univ3seth2_univ3_nft_mints.fee
  AND json_query(params, 'lax $.tickLower') = univ3seth2_univ3_nft_mints.tickLower
  AND json_query(params, 'lax $.tickUpper') = univ3seth2_univ3_nft_mints.tickUpper
  WHERE call_success
), univ3seth2_liquidity_movements_1 AS (
    SELECT
      tokenId,
      is_owned,
      wallet,
      token0,
      token1,
      fee,
      tickLower,
      tickUpper,
      mint_ts,
      ts,
      hash,
      amount0,
      amount1,
      liquidity
    FROM
      univ3seth2_same_pool_nft_mints
      JOIN (
        SELECT
          tokenId,
          evt_block_time AS ts,
          evt_tx_hash AS hash,
          CAST(amount0 AS INT256) AS amount0,
          CAST(amount1 AS INT256) AS amount1,
          CAST(liquidity AS INT256) AS liquidity
        FROM
          uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_IncreaseLiquidity
        UNION ALL
        SELECT
          tokenId,
          evt_block_time AS ts,
          evt_tx_hash AS hash,
          - CAST(amount0 AS INT256) AS amount0,
          - CAST(amount1 AS INT256) AS amount1,
          - CAST(liquidity AS INT256) AS liquidity
        FROM
          uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_DecreaseLiquidity
      ) USING (tokenId)
    WHERE
      ts >= mint_ts
    UNION ALL
    SELECT
      tokenId,
      /* might be better to put NULL here instead */ 0 AS is_owned,
      wallet,
      token0,
      token1,
      fee,
      tickLower,
      tickUpper,
      mint_ts,
      ts,
      NULL AS hash,
      TRY_CAST(NULL AS INT256) AS amount0,
      TRY_CAST(NULL AS INT256) AS amount1,
      TRY_CAST(NULL AS INT256) AS liquidity
    FROM
      univ3seth2_univ3_nft_mints
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
      ) ON ts > CAST(mint_ts AS DATE)
  ),
  univ3seth2_liquidity_movements_2 AS (
    SELECT
      tokenId,
      is_owned,
      wallet,
      token0,
      token1,
      fee,
      tickLower,
      tickUpper,
      CASE
        WHEN hash IS NULL THEN ts - INTERVAL '1' SECOND
        ELSE ts
      END AS ts,
      hash,
      amount0,
      amount1,
      liquidity,
      rate,
      amount0 * is_owned AS net_deposit_amount_weth,
      amount1 * is_owned AS net_deposit_amount_seth2,
      (amount0 + amount1) * is_owned AS net_deposit_amount,
      SUM(is_owned * liquidity) OVER (
        PARTITION BY
          wallet,
          token0,
          token1,
          fee,
          tickLower,
          tickUpper
        ORDER BY
          ts
      ) AS cumulative_liquidity,
      MAX(rate) OVER (
        PARTITION BY
          rate_grp
      ) AS rate_filled
    FROM
      (
        SELECT
          *,
          CAST((amount0 + amount1) AS DOUBLE) / liquidity AS rate,
          SUM(
            CASE
              WHEN NOT liquidity IS NULL THEN 1
              ELSE 0
            END
          ) OVER (
            ORDER BY
              ts,
              CAST((amount0 + amount1) AS DOUBLE) / liquidity
          ) AS rate_grp
        FROM
          univ3seth2_liquidity_movements_1
      )
  ),
  univ3seth2_liquidity_movements_3 AS (
    SELECT
      tokenId,
      wallet,
      token0,
      token1,
      fee,
      tickLower,
      tickUpper,
      ts,
      hash,
      cumulative_liquidity,
      rate_filled AS rate,
      net_deposit_amount_weth / 1e18 AS net_deposit_amount_weth,
      net_deposit_amount_seth2 / 1e18 AS net_deposit_amount_seth2,
      net_deposit_amount / 1e18 AS net_deposit_amount,
      (
        rate - LAG(rate_filled) OVER (
          PARTITION BY
            wallet,
            token0,
            token1,
            fee,
            tickLower,
            tickUpper
          ORDER BY
            ts
        )
      ) * cumulative_liquidity / 1e18 AS il_changes_since_last_update,
      univ3seth2_prices.price,
      univ3seth2_prices.contract_address AS token_address,
      /* only WETH in there so this works */ cumulative_liquidity * rate_filled / 1e18 AS balance
    FROM
      univ3seth2_liquidity_movements_2
      LEFT JOIN univ3seth2_prices ON period = CAST(ts AS DATE)
  ),
  univ3seth2_collects AS (
    SELECT
      'COLLECTS-UNIV3-' || CASE
        WHEN collect.contract_address = 0x7379e81228514a1d2a6cf7559203998e20598346 THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          collect.contract_address
        ORDER BY
          evt_block_time,
          wallet,
          amount0 + amount1
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN collect.contract_address = 0x7379e81228514a1d2a6cf7559203998e20598346 THEN 121
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      evt_block_time AS ts,
      amount0 / 1e18 * price AS amount,
      univ3seth2_prices.contract_address AS token_address,
      amount0 / 1e18 AS token_amount,
      CAST(evt_tx_hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      uniswap_v3_ethereum.Pair_evt_Collect AS collect
      JOIN univ3seth2_wallet_list ON recipient = wallet
      LEFT JOIN univ3seth2_prices ON period = CAST(evt_block_time AS DATE)
    UNION ALL
    SELECT
      'COLLECTS-UNIV3-' || CASE
        WHEN collect.contract_address = 0x7379e81228514a1d2a6cf7559203998e20598346 THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          collect.contract_address
        ORDER BY
          evt_block_time,
          wallet,
          amount0 + amount1
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN collect.contract_address = 0x7379e81228514a1d2a6cf7559203998e20598346 THEN 131005
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      evt_block_time AS ts,
      amount1 / 1e18 * price AS amount,
      univ3seth2_prices.contract_address AS token_address,
      amount1 / 1e18 AS token_amount,
      CAST(evt_tx_hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      uniswap_v3_ethereum.Pair_evt_Collect AS collect
      JOIN univ3seth2_wallet_list ON recipient = wallet
      LEFT JOIN univ3seth2_prices ON period = CAST(evt_block_time AS DATE)
    UNION ALL
    SELECT
      'COLLECTS-UNIV3-' || CASE
        WHEN collect.contract_address = 0x7379e81228514a1d2a6cf7559203998e20598346 THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          collect.contract_address
        ORDER BY
          evt_block_time,
          wallet,
          amount0 + amount1
      ) AS VARCHAR) AS transaction,
      3 AS line,
      CASE
        WHEN collect.contract_address = 0x7379e81228514a1d2a6cf7559203998e20598346 THEN 32321005
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      evt_block_time AS ts,
      (amount0 + amount1) / 1e18 * price AS amount,
      univ3seth2_prices.contract_address AS token_address,
      (amount0 + amount1) / 1e18 AS token_amount,
      CAST(evt_tx_hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      uniswap_v3_ethereum.Pair_evt_Collect AS collect
      JOIN univ3seth2_wallet_list ON recipient = wallet
      LEFT JOIN univ3seth2_prices ON period = CAST(evt_block_time AS DATE)
  ),
  univ3seth2_accounting AS (
    SELECT
      'DEPWITH-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          net_deposit_amount
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 131003
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      ts,
      net_deposit_amount * price AS amount,
      token_address,
      net_deposit_amount AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      univ3seth2_liquidity_movements_3
    WHERE
      COALESCE(net_deposit_amount, 0) <> 0
    UNION ALL
    SELECT
      'DEPWITH-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          net_deposit_amount
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 121
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      ts,
      - net_deposit_amount_weth * price AS amount,
      token_address,
      - net_deposit_amount_weth AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      univ3seth2_liquidity_movements_3
    WHERE
      COALESCE(net_deposit_amount, 0) <> 0
    UNION ALL
    SELECT
      'DEPWITH-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          net_deposit_amount
      ) AS VARCHAR) AS transaction,
      3 AS line,
      CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 131005
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      ts,
      - net_deposit_amount_seth2 * price AS amount,
      token_address,
      - net_deposit_amount_seth2 AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      univ3seth2_liquidity_movements_3
    WHERE
      COALESCE(net_deposit_amount, 0) <> 0
    UNION ALL
    SELECT
      'IL-UPDATES-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          rate
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 131003
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      ts,
      il_changes_since_last_update * price AS amount,
      token_address,
      il_changes_since_last_update AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      univ3seth2_liquidity_movements_3
    WHERE
      COALESCE(il_changes_since_last_update, 0) <> 0
    UNION ALL
    SELECT
      'IL-UPDATES-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          rate
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 32321005
        ELSE 999999
      END AS account,
      'P&L' AS ledger,
      ts,
      il_changes_since_last_update * price AS amount,
      token_address,
      il_changes_since_last_update AS token_amount,
      CAST(hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      univ3seth2_liquidity_movements_3
    WHERE
      COALESCE(il_changes_since_last_update, 0) <> 0
    UNION ALL
    SELECT
      *
    FROM
      univ3seth2_collects
    UNION ALL
    SELECT
      'M2M-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          rate
      ) AS VARCHAR) AS transaction,
      1 AS line,
      131003 AS account,
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
          univ3seth2_liquidity_movements_3
        WHERE
          hash IS NULL
      ) AS sub
    WHERE
      COALESCE(lag_balance * (price - lag_price), 0) <> 0
    UNION ALL
    SELECT
      'M2M-UNIV3-' || CASE
        WHEN token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND token1 = '0xfe2e637202056d30016725477c5da089ab0a043a' THEN 'SETH2-WETH-'
        ELSE 'Unknown-'
      END || CAST(ROW_NUMBER() OVER (
        PARTITION BY
          token0,
          token1
        ORDER BY
          ts,
          wallet,
          rate
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
          univ3seth2_liquidity_movements_3
        WHERE
          hash IS NULL
      ) AS sub
    WHERE
      COALESCE(lag_balance * (price - lag_price), 0) <> 0
  )
SELECT
  *
FROM
  univ3seth2_accounting
ORDER BY
  ts,
  transaction,
  line
eth2_accounting
ORDER BY
  ts,
  transaction,
  line