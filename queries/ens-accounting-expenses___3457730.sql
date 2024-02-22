-- WARNING: this query may be part of multiple repos
-- part of a query repo
-- query name: ENS-Accounting-Expenses
-- query link: https://dune.com/queries/3457730


WITH
  expenses_wallets AS (
    SELECT
      0xfe89cc7abb2c4183683ab71653c4cdc9b02d44b7 AS wallet,
      'DAO Wallet' AS name
    UNION ALL
    SELECT
      0xcf60916b6cb4753f58533808fa610fcbd4098ec0 AS wallet,
      'ENS: Gnosis Safe' AS name
  )
  /*    union ALL -- Not added here but sterilized on all expenses
  SELECT '0x4f2083f5fbede34c2714affb3105539775f7fe64' AS wallet, 'ENS: EnDAOment' AS name
   */
,
  expenses_milkmen AS (
    SELECT
      trader,
      tx_hash
    FROM
      cow_protocol_ethereum.trades
    WHERE
      receiver IN (
        SELECT
          wallet
        FROM
          expenses_wallets
      )
  ),
  expenses_swap_hashes AS (
    SELECT
      tx_hash,
      1 AS is_trade
    FROM
      expenses_milkmen
    UNION ALL
    SELECT
      tx_hash,
      1 AS is_trade
    FROM
      dex.trades
    WHERE
      blockchain = 'ethereum'
      AND taker IN (
        SELECT
          wallet
        FROM
          expenses_wallets
      )
  ),
  expenses_counterparties AS (
    SELECT
      *
    FROM
      (
        VALUES
          (
            0x690f0581ececcf8389c223170778cd9d029606f2,
            'SUPP',
            1,
            'True Names Ltd'
          ),
          (
            0xF29Ff96aaEa6C9A1fBa851f74737f3c069d4f1a9,
            'GRANT',
            1,
            'Protocol Guild'
          ),
          (
            0x91c32893216de3ea0a55abb9851f581d4503d39b,
            'WG',
            1,
            'Meta-DAO WG'
          ),
          (
            0x2686a8919df194aa7673244549e68d42c1685d03,
            'WG',
            2,
            'Ecosystem WG'
          ),
          (
            0xcd42b4c4d102cc22864e3a1341bb0529c17fd87d,
            'WG',
            3,
            'Public Goods WG'
          )
      ) AS t (
        counterparty,
        counterparty_type,
        counterparty_id,
        counterparty_label
      )
  ),
  expenses_tokens AS (
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
        0xc18360217d8f7ab5e7c516566761ea12ce7f9d72,
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      )
  ),
  expenses_prices AS (
    SELECT
      DATE_TRUNC('day', minute) AS period,
      contract_address,
      price
    FROM
      prices.usd
      INNER JOIN expenses_tokens USING (contract_address)
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
      AND NOT contract_address IN (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48) /* USDC */
    UNION ALL
    SELECT
      period,
      0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS contract_address,
      1.0 AS price
    FROM
      UNNEST (
        SEQUENCE(
          CAST(
            SUBSTR(CAST('2019-01-01' AS VARCHAR), 1, 10) AS DATE
          ),
          CURRENT_DATE - INTERVAL '1' day,
          INTERVAL '1' day
        )
      ) AS _u (period)
  ),
  expenses_tokenflows AS (
    SELECT
      evt_block_time AS ts,
      evt_tx_hash AS tx_hash,
      DATE_TRUNC('day', evt_block_time) AS period,
      wallet,
      "to" AS counterparty,
      contract_address,
      - CAST(value AS DOUBLE) AS qty
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN expenses_tokens USING (contract_address)
      INNER JOIN expenses_wallets ON "from" = wallet
      LEFT JOIN expenses_swap_hashes ON tx_hash = evt_tx_hash
    WHERE
      evt_block_time > CAST('2019-01-01' AS TIMESTAMP)
      AND NOT to IN (
        SELECT
          trader
        FROM
          expenses_milkmen
      ) /* excluding transactions to relevant milkman contracts as these are just parts of swaps */
      AND NOT to IN (
        SELECT
          wallet
        FROM
          expenses_wallets
      ) /* exclude internal transfers */
      AND to <> 0x4f2083f5fbede34c2714affb3105539775f7fe64
      AND expenses_swap_hashes.is_trade IS NULL /* exclude swaps */
    UNION ALL
    SELECT
      block_time AS ts,
      tx_hash,
      DATE_TRUNC('day', block_time) AS period,
      wallet,
      "to" AS counterparty,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS contract_address,
      - CAST(value AS DOUBLE) AS qty
    FROM
      ethereum.traces
      INNER JOIN expenses_wallets ON "from" = wallet
      LEFT JOIN expenses_swap_hashes USING (tx_hash)
    WHERE
      success = TRUE
      AND (
        NOT call_type IN ('delegatecall', 'callcode', 'staticcall')
        OR call_type IS NULL
      )
      AND NOT to IN (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2) /* WETH, doesn't have ERC20 mint */
      AND to <> 0x4f2083f5fbede34c2714affb3105539775f7fe64
      AND NOT to IN (
        SELECT
          trader
        FROM
          expenses_milkmen
      ) /* excluding transactions to relevant milkman contracts as these are just parts of swaps */
      AND NOT to IN (
        SELECT
          wallet
        FROM
          expenses_wallets
      ) /* exclude internal transfers */
      AND expenses_swap_hashes.is_trade IS NULL /* exclude swaps */
      AND block_time > CAST('2019-01-01' AS TIMESTAMP)
  ),
  expenses_accounting AS (
    /* expenses */
    SELECT
      'EXP' || CAST (RANK() OVER (
        ORDER BY
          ts NULLS FIRST,
          wallet NULLS FIRST,
          contract_address NULLS FIRST,
          counterparty NULLS FIRST
      ) AS VARCHAR ) AS transaction,
      1 AS line,
      CAST(CASE
        WHEN counterparty_type = 'WG' THEN '3221' || LPAD(CAST(counterparty_id AS VARCHAR), 3, '0') /* Recoognized working groups */
        WHEN counterparty_type = 'GRANT' THEN '3222' || LPAD(CAST(counterparty_id AS VARCHAR), 3, '0') /* Recoognized grants address */
        WHEN counterparty_type = 'SUPP' THEN '3223' || LPAD(CAST(counterparty_id AS VARCHAR), 3, '0') /* Recoognized suppliers */
        ELSE '3229'
      END AS BIGINT)
      AS account,
      'ONCHAIN' AS ledger,
      ts AS period,
      qty * price / divisor AS amount,
      contract_address AS token_address,
      qty / divisor AS token_amount,
      CAST(tx_hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      expenses_tokenflows
      LEFT JOIN expenses_counterparties USING (counterparty)
      LEFT JOIN expenses_prices USING (contract_address, period)
      LEFT JOIN expenses_tokens USING (contract_address)
    WHERE
      qty <> 0
    UNION ALL
    /* decrease of assets */
    SELECT
      'EXP' || CAST(RANK() OVER (
        ORDER BY
          ts NULLS FIRST,
          wallet NULLS FIRST,
          contract_address NULLS FIRST,
          counterparty NULLS FIRST
      ) AS VARCHAR ) AS transaction,
      2 AS line,
      CASE contract_address
        WHEN 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 THEN 111001 /* USDC => Cash */
        WHEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121 /* ETH => ETH */
        WHEN 0xc18360217d8f7ab5e7c516566761ea12ce7f9d72 THEN 312 /* ENS trxns handled separately */
        ELSE 129 /* Other currencies */
      END AS account,
      'ONCHAIN' AS ledger,
      ts AS period,
      CASE
        WHEN contract_address = 0xc18360217d8f7ab5e7c516566761ea12ce7f9d72 THEN - qty * price / divisor
        ELSE qty * price / divisor
      END AS amount,
      contract_address AS token_address,
      CASE
        WHEN contract_address = 0xc18360217d8f7ab5e7c516566761ea12ce7f9d72 THEN - qty / divisor
        ELSE qty / divisor
      END AS token_amount,
      CAST(tx_hash AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      expenses_tokenflows
      LEFT JOIN expenses_counterparties USING (counterparty)
      LEFT JOIN expenses_prices USING (contract_address, period)
      LEFT JOIN expenses_tokens USING (contract_address)
    WHERE
      qty <> 0
  )
SELECT
  *
FROM
  expenses_accounting