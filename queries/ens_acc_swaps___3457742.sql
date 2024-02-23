-- part of a query repo
-- query name: ENS-Accounting-Swaps
-- query link: https://dune.com/queries/3457742


WITH swaps_wallets AS 
(
    SELECT * FROM 
    (
        VALUES
        (0xfe89cc7abb2c4183683ab71653c4cdc9b02d44b7, 'ENS: DAO Wallet')
        , (0xcf60916b6cb4753f58533808fa610fcbd4098ec0, 'ENS: Gnosis Safe')
        , (0x911143d946ba5d467bfc476491fdb235fef4d667, 'ENS: Multisig')
        , (0x4f2083f5fbede34c2714affb3105539775f7fe64, 'ENS: EnDAOment')
    ) AS t(wallet, name)

    UNION ALL
    
    SELECT
      contract_address AS wallet,
      'ENS: Registrar Controller ' || TRY_CAST(
        ROW_NUMBER() OVER (
          ORDER BY
            min_ts
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
  swaps_tokens AS (
    SELECT
      contract_address,
      symbol,
      decimals,
      POWER(10, decimals) AS divisor,
      CASE
        WHEN contract_address IN (
          0xc0c293ce456ff0ed870add98a0828dd4d2903dbf,
          0xba100000625a3754423978a60c9317c58a424e3d,
          0x5a98fcbea516cf06857215779fd812ca3bef1b32,
          0xc00e94cb662c3520282e6f5717214004a7f26888
        ) THEN 1
        ELSE 0
      END AS is_reward_token
    FROM
      tokens.erc20
    WHERE
      blockchain = 'ethereum'
      AND contract_address IN (
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
        0x6b175474e89094c44da98b954eedeac495271d0f,
        0xc18360217d8f7ab5e7c516566761ea12ce7f9d72,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
        0xfe2e637202056d30016725477c5da089ab0a043a,
        0xae7ab96520de3a18e5e111b5eaab095312d7fe84,
        0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0,
        0xc0c293ce456ff0ed870add98a0828dd4d2903dbf,
        0xba100000625a3754423978a60c9317c58a424e3d,
        0x5a98fcbea516cf06857215779fd812ca3bef1b32,
        0xc00e94cb662c3520282e6f5717214004a7f26888,
        0xae78736cd615f374d3085123a210448e74fc6393
      )
    UNION ALL
    SELECT
      0x0000000000000000000000000000000000000000 AS contract_address,
      /* ETH SOMETIMES SHOWS UP AS THIS IN DEX.TRADES */ 'WETH' AS symbol,
      18 AS decimals,
      POWER(10, 18) AS divisor,
      0 AS is_reward_token
  ),
  swaps_prices AS (
    SELECT
      DATE_TRUNC('DAY', minute) AS period,
      contract_address,
      price
    FROM
      prices.usd
      JOIN swaps_tokens USING (contract_address)
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
      AND NOT contract_address IN (
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
        0x6b175474e89094c44da98b954eedeac495271d0f,
        0xfe2e637202056d30016725477c5da089ab0a043a,
        0xae7ab96520de3a18e5e111b5eaab095312d7fe84,
        0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0,
        0xae78736cd615f374d3085123a210448e74fc6393
      ) /* USDC, DAI, sETH2, stETH, wstETH, rETH */
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
          CURRENT_DATE - INTERVAL '1' DAY,
          INTERVAL '1' DAY
        ) /* WARNING: Check out the docs for example of time series generation: https://dune.com/docs/query/syntax-differences/ */
      ) AS _u (period)
    UNION ALL
    SELECT
      period,
      0x6b175474e89094c44da98b954eedeac495271d0f AS contract_address,
      1.0 AS price
    FROM
      UNNEST (
        SEQUENCE(
          CAST(
            SUBSTR(CAST('2018-01-01' AS VARCHAR), 1, 10) AS DATE
          ),
          CURRENT_DATE - INTERVAL '1' DAY,
          INTERVAL '1' DAY
        ) /* WARNING: Check out the docs for example of time series generation: https://dune.com/docs/query/syntax-differences/ */
      ) AS _u (period)
  ),
  swaps_dex_trades AS (
    SELECT
      tr.block_time AS ts,
      CASE
        WHEN token_bought_address = 0x0000000000000000000000000000000000000000 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE token_bought_address
      END AS token_bought_address,
      CASE
        WHEN token_sold_address = 0x0000000000000000000000000000000000000000 THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE token_sold_address
      END AS token_sold_address,
      COALESCE(
        token_bought_amount,
        CAST(token_bought_amount_raw AS INT256) / b.divisor
      ) AS token_bought_amount,
      /* token_bought_amount is NULL sometimes when token_bought_address is the burn address representing ETH */ COALESCE(
        token_sold_amount,
        CAST(token_sold_amount_raw AS INT256) / s.divisor
      ) AS token_sold_amount,
      /* token_sold_amount is NULL sometimes when token_sold_address is the burn address representing ETH */ tx_hash AS reference,
      wallet
    FROM
      dex.trades AS tr
      JOIN swaps_wallets ON taker = wallet
      JOIN swaps_tokens AS b ON tr.token_bought_address = b.contract_address
      JOIN swaps_tokens AS s ON tr.token_sold_address = s.contract_address
    WHERE
      blockchain = 'ethereum'
      AND NOT token_bought_amount_raw IS NULL
      AND NOT token_sold_amount_raw IS NULL
      AND NOT (
        project = 'balancer'
        AND version = '2'
      ) /* balancer_v2 trades assume the transaction sender is the taker in this table. not always the case */
    UNION ALL
    
    SELECT tr.call_block_time AS ts
    , b.contract_address AS token_bought_address
    , s.contract_address AS token_sold_address
    , output_amountCalculated/b.divisor AS token_bought_amount
    , CAST(json_query(tr.singleSwap, 'lax $.amount') AS INT256)/s.divisor AS token_sold_amount
    , call_tx_hash AS reference
    , wallet
    FROM balancer_v2_ethereum.Vault_call_swap tr
    JOIN swaps_wallets
    ON json_query(tr.funds, 'lax $.recipient' OMIT QUOTES) = CAST(wallet AS VARCHAR)
    JOIN swaps_tokens b
    ON json_query(tr.singleSwap, 'lax $.assetOut' OMIT QUOTES) = CAST(b.contract_address AS VARCHAR)
    JOIN swaps_tokens s
    ON json_query(tr.singleSwap, 'lax $.assetIn' OMIT QUOTES)  = CAST(s.contract_address AS VARCHAR)
    WHERE tr.call_success
    
    UNION ALL
    
    SELECT
      tr.block_time AS ts,
      buy_token_address AS token_bought_address,
      sell_token_address AS token_sold_address,
      units_bought AS token_bought_amount,
      units_sold + fee AS token_sold_amount,
      tx_hash AS reference,
      wallet
    FROM
      cow_protocol_ethereum.trades AS tr
      JOIN swaps_wallets ON receiver = wallet
      JOIN swaps_tokens AS b ON tr.buy_token_address = b.contract_address
      JOIN swaps_tokens AS s ON tr.sell_token_address = s.contract_address

    UNION ALL
    
    SELECT
      tr.evt_block_time AS ts,
      tr.tokenOut AS token_bought_address,
      tr.tokenIn AS token_sold_address,
      tr.amountOut/b.divisor AS token_bought_amount,
      tr.amountIn/s.divisor  AS token_sold_amount,
      tr.evt_tx_hash AS reference,
      wallet
    FROM
      sushi_ethereum.RouteProcessor3_evt_Route AS tr --adding in sushi aggregator trades
      JOIN swaps_wallets ON to = wallet --from and to should both be the endaoment address
      JOIN swaps_tokens AS b ON tr.tokenOut = b.contract_address
      JOIN swaps_tokens AS s ON tr.tokenIn = s.contract_address
  ),
  swaps_dex_trades_2 AS (
    SELECT
      ts,
      CASE
        WHEN token_bought_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 THEN 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
        ELSE token_bought_address
      END AS token_bought_address,
      /* wsteth = steth */ CASE
        WHEN token_sold_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 THEN 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
        ELSE token_sold_address
      END AS token_sold_address,
      /* wsteth = steth */ CASE
        WHEN token_bought_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 THEN token_bought_amount * rate
        ELSE token_bought_amount
      END AS token_bought_amount,
      CASE
        WHEN token_sold_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 THEN token_sold_amount * rate
        ELSE token_sold_amount
      END AS token_sold_amount,
      reference,
      wallet
    FROM
      (
        SELECT
          tr.*,
          rt.postTotalPooledEther / rt.totalShares AS rate,
          ROW_NUMBER() OVER (
            PARTITION BY
              tr.ts, tr.reference, tr.token_bought_amount
            ORDER BY
              rt.evt_block_time DESC
          ) AS rn
        FROM
          swaps_dex_trades AS tr
          LEFT JOIN lido_ethereum.LegacyOracle_evt_PostTotalShares AS rt ON tr.ts >= rt.evt_block_time
        WHERE
          token_bought_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0
          OR token_sold_address = 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0
      )
    WHERE
      rn = 1
    UNION ALL

    SELECT
      ts, token_bought_address, token_sold_address
      , CASE WHEN token_bought_address = 0xae78736cd615f374d3085123a210448e74fc6393 THEN rate ELSE 1 END * token_bought_amount as token_bought_amount
      , CASE WHEN token_sold_address = 0xae78736cd615f374d3085123a210448e74fc6393 THEN rate ELSE 1 END * token_sold_amount   as token_sold_amount,
      reference,
      wallet
    FROM
      (
        SELECT
          tr.*,
          cast(reth_rt.totalEth AS DOUBLE)/CAST(reth_rt.rethSupply as double) AS rate,
          ROW_NUMBER() OVER (
            PARTITION BY
              tr.ts, tr.reference, tr.token_bought_amount
            ORDER BY
              reth_rt.evt_block_time DESC
          ) AS rn
        FROM
          swaps_dex_trades AS tr
          LEFT JOIN rocketpool_ethereum.RocketNetworkBalances_evt_BalancesUpdated AS reth_rt ON tr.ts >= reth_rt.evt_block_time
        WHERE
          token_bought_address = 0xae78736cd615f374d3085123a210448e74fc6393
          OR token_sold_address = 0xae78736cd615f374d3085123a210448e74fc6393
      )
    WHERE
      rn = 1

    UNION ALL
    SELECT
      *
    FROM
      swaps_dex_trades
    WHERE
      token_bought_address NOT IN (0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 0xae78736cd615f374d3085123a210448e74fc6393)
      AND token_sold_address NOT IN (0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 0xae78736cd615f374d3085123a210448e74fc6393)
  ),
  swaps_unioned AS (
    SELECT
      'SWAP' || CAST(RANK() OVER (
        ORDER BY
          ts,
          token_bought_address,
          token_bought_amount
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token_bought_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 THEN 111001 /* USDC */
        WHEN token_bought_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 111002 /* DAI */
        WHEN token_bought_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121 /* WETH */
        WHEN token_bought_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 THEN 131004 /* stETH */
        WHEN token_bought_address = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 131005 /* sETH2 */
        WHEN token_bought_address = 0xae78736cd615f374d3085123a210448e74fc6393 THEN 131007 /* rETH */
        ELSE 129 /* other cryptos */
      END AS account,
      'SWAPS' AS ledger,
      ts,
      CASE
        WHEN token_bought_address IN (
          0xae7ab96520de3a18e5e111b5eaab095312d7fe84,
          0xfe2e637202056d30016725477c5da089ab0a043a,
          0xae78736cd615f374d3085123a210448e74fc6393
        ) /* treating forms of eth the same */ THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        WHEN token_bought_address IN (0x6b175474e89094c44da98b954eedeac495271d0f) THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_bought_address
      END AS token_address,
      token_bought_amount AS token_amount,
      CAST(reference AS VARCHAR) AS reference,
      CASt(wallet AS VARCHAR) AS wallet
    FROM
      swaps_dex_trades_2
    WHERE
      NOT token_sold_address IN (
        SELECT
          contract_address
        FROM
          swaps_tokens
        WHERE
          is_reward_token = 1
      )
    UNION ALL
    SELECT
      'SWAP' || CAST(RANK() OVER (
        ORDER BY
          ts,
          token_bought_address,
          token_bought_amount
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token_sold_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 THEN 111001 /* USDC */
        WHEN token_sold_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 111002 /* DAI */
        WHEN token_sold_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121 /* WETH */
        WHEN token_sold_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 THEN 131004 /* stETH */
        WHEN token_sold_address = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 131005 /* sETH2 */
        WHEN token_sold_address = 0xae78736cd615f374d3085123a210448e74fc6393 THEN 131007 /* rETH */
        ELSE 129 /* other cryptos */
      END AS account,
      'SWAPS' AS ledger,
      ts,
      CASE
        WHEN token_sold_address IN (
          0xae7ab96520de3a18e5e111b5eaab095312d7fe84,
          0xfe2e637202056d30016725477c5da089ab0a043a,
          0xae78736cd615f374d3085123a210448e74fc6393
        ) /* treating forms of eth the same */ THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        WHEN token_sold_address IN (0x6b175474e89094c44da98b954eedeac495271d0f) THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_sold_address
      END AS token_address,
      - token_sold_amount AS token_amount,
      CAST(reference AS VARCHAR) AS reference,
      CASt(wallet AS VARCHAR) AS wallet
    FROM
      swaps_dex_trades_2
    WHERE
      NOT token_sold_address IN (
        SELECT
          contract_address
        FROM
          swaps_tokens
        WHERE
          is_reward_token = 1
      )
  ),
  swaps_accounting_1 AS (
    SELECT
      transaction,
      line,
      account,
      ledger,
      ts,
      token_amount * price AS amount,
      token_address,
      token_amount,
      reference,
      wallet
    FROM
      swaps_unioned
      LEFT JOIN swaps_prices ON swaps_unioned.token_address = swaps_prices.contract_address
      AND DATE_TRUNC('DAY', swaps_unioned.ts) = swaps_prices.period
  ),
  swaps_accounting AS (
    SELECT
      *
    FROM
      swaps_accounting_1
    UNION ALL
    SELECT
      transaction,
      3 AS line,
      CASE
        WHEN max_token_address = min_token_address THEN 32323
        ELSE 3233
      END AS account,
      /* when it's a like for like swap, this counts toward "farming" PnL, otherwise does not */ 'SWAPS' AS ledger,
      ts,
      sum_amount AS amount,
      CASE
        WHEN max_token_address = min_token_address THEN max_token_address
        ELSE 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
      END AS token_address,
      /* USDC */ CASE
        WHEN max_token_address = min_token_address THEN sum_token_amount
        ELSE 0
      END AS token_amount,
      reference,
      wallet
    FROM
      (
        SELECT
          transaction,
          reference,
          ts,
          wallet,
          SUM(amount) AS sum_amount,
          SUM(token_amount) AS sum_token_amount,
          MAX(token_address) AS max_token_address,
          MIN(token_address) AS min_token_address
        FROM
          swaps_accounting_1
        GROUP BY
          1,
          2,
          3,
          4
      ) AS sub
    UNION ALL
    SELECT
      'RWRD-' || symbol || '-' || CAST(RANK() OVER (
        PARTITION BY
          token_sold_address
        ORDER BY
          ts,
          token_bought_address,
          token_bought_amount
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token_bought_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 THEN 111001 /* USDC */
        WHEN token_bought_address = 0x6b175474e89094c44da98b954eedeac495271d0f THEN 111002 /* DAI */
        WHEN token_bought_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121 /* WETH */
        WHEN token_bought_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 THEN 131004 /* stETH */
        WHEN token_bought_address = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 131005 /* sETH2 */
        WHEN token_bought_address = 0xae78736cd615f374d3085123a210448e74fc6393 THEN 131007 /* rETH */
        ELSE 129 /* other cryptos */
      END AS account,
      'RWRDS' AS ledger,
      ts,
      token_bought_amount * price AS amount,
      CASE
        WHEN token_bought_address IN (
          0xae7ab96520de3a18e5e111b5eaab095312d7fe84,
          0xfe2e637202056d30016725477c5da089ab0a043a,
          0xae78736cd615f374d3085123a210448e74fc6393
        ) /* treating forms of eth the same */ THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        WHEN token_bought_address IN (0x6b175474e89094c44da98b954eedeac495271d0f) THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_bought_address
      END AS token_address,
      token_bought_amount AS token_amount,
      CAST(reference AS VARCHAR) AS reference,
      CASt(wallet AS VARCHAR) AS wallet
    FROM
      swaps_dex_trades_2
      JOIN (
        SELECT
          *
        FROM
          swaps_tokens
        WHERE
          is_reward_token = 1
      ) AS reward_tokens ON swaps_dex_trades_2.token_sold_address = reward_tokens.contract_address
      LEFT JOIN swaps_prices ON swaps_dex_trades_2.token_bought_address = swaps_prices.contract_address
      AND DATE_TRUNC('DAY', swaps_dex_trades_2.ts) = swaps_prices.period
    UNION ALL
    SELECT
      'RWRD-' || symbol || '-' || CAST(RANK() OVER (
        PARTITION BY
          token_sold_address
        ORDER BY
          ts,
          token_bought_address,
          token_bought_amount
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token_sold_address = 0xc00e94cb662c3520282e6f5717214004a7f26888 THEN 32322001 /* COMP */
        WHEN token_sold_address = 0xc0c293ce456ff0ed870add98a0828dd4d2903dbf THEN 32322002 /* AURA */
        WHEN token_sold_address = 0xba100000625a3754423978a60c9317c58a424e3d THEN 32322003 /* BAL */
        WHEN token_sold_address = 0x5a98fcbea516cf06857215779fd812ca3bef1b32 THEN 32322004 /* LDO */
        ELSE 999999 /* ERROR catchall */
      END AS account,
      'RWRDS' AS ledger,
      ts,
      token_bought_amount * price AS amount,
      CASE
        WHEN token_bought_address IN (
          0xae7ab96520de3a18e5e111b5eaab095312d7fe84,
          0xfe2e637202056d30016725477c5da089ab0a043a,
          0xae78736cd615f374d3085123a210448e74fc6393
        ) /* treating forms of eth the same */ THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        WHEN token_bought_address IN (0x6b175474e89094c44da98b954eedeac495271d0f) THEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        ELSE token_bought_address
      END AS token_address,
      token_bought_amount AS token_amount,
      CAST(reference AS VARCHAR) AS reference,
      CASt(wallet AS VARCHAR) AS wallet
    FROM
      swaps_dex_trades_2
      JOIN (
        SELECT
          *
        FROM
          swaps_tokens
        WHERE
          is_reward_token = 1
      ) AS reward_tokens ON swaps_dex_trades_2.token_sold_address = reward_tokens.contract_address
      LEFT JOIN swaps_prices ON swaps_dex_trades_2.token_bought_address = swaps_prices.contract_address
      AND DATE_TRUNC('DAY', swaps_dex_trades_2.ts) = swaps_prices.period
  )
SELECT
  *
FROM
  swaps_accounting
WHERE
  CAST(ts AS DATE) < CURRENT_DATE
ORDER BY
  ts