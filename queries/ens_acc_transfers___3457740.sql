-- part of a query repo
-- query name: ENS-Accounting-Transfers
-- query link: https://dune.com/queries/3457740

WITH
  transfers_wallets AS (
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
            contract_address
            , MIN(evt_block_time) AS min_ts
        FROM
            ens.view_registrations
        GROUP BY 1
        UNION ALL
        SELECT contract_address
            , MIN(evt_block_time) as mint_ts
        FROM ethereumnameservice_ethereum.ETHRegistrarController_4_evt_NameRegistered
        GROUP BY 1
      ) AS sub
  ),
  transfers_tokens AS (
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
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
        0xfe2e637202056d30016725477c5da089ab0a043a,
        0xae7ab96520de3a18e5e111b5eaab095312d7fe84
      )
  ),
  transfers_prices AS (
    SELECT
      DATE_TRUNC('DAY', minute) AS period,
      contract_address,
      price
    FROM
      prices.usd
      JOIN transfers_tokens USING (contract_address)
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
        0xfe2e637202056d30016725477c5da089ab0a043a,
        0xae7ab96520de3a18e5e111b5eaab095312d7fe84
      ) /* USDC, SETH2, stETH */
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
          CURRENT_DATE - INTERVAL '1' DAY,
          INTERVAL '1' DAY
        )
      ) AS _u (period)
  ),
  transfers_wallet_transfers AS (
    SELECT
      tr.block_time AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address_specific,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      CAST(value AS DOUBLE) / 1e18 AS token_amount,
      tx_hash AS reference,
      f.wallet AS from_wallet,
      t.wallet AS to_wallet,
      f.name AS from_name,
      t.name AS to_name
    FROM
      ethereum.traces AS tr
      JOIN transfers_wallets AS f ON "from" = f.wallet
      JOIN transfers_wallets AS t ON "to" = t.wallet
    WHERE
      success = TRUE
      AND CAST(value AS DOUBLE) + 0 > 0 /* need to add 0 because it's stored as a string */
      AND (
        NOT call_type IN ('delegatecall', 'callcode', 'staticcall')
        OR call_type IS NULL
      )
    UNION ALL
    SELECT
      tr.evt_block_time AS ts,
      contract_address AS token_address_specific,
      CASE
        WHEN contract_address IN (
          0xfe2e637202056d30016725477c5da089ab0a043a,
          0xae7ab96520de3a18e5e111b5eaab095312d7fe84
        ) THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ELSE contract_address
      END AS token_address,
      /* equating stETH to weth */ CAST(value AS DOUBLE) / POWER(10, transfers_tokens.decimals) AS token_amount,
      evt_tx_hash AS reference,
      f.wallet AS from_wallet,
      t.wallet AS to_wallet,
      f.name AS from_name,
      t.name AS to_name
    FROM
      erc20_ethereum.evt_Transfer AS tr
      JOIN transfers_wallets AS f ON "from" = f.wallet
      JOIN transfers_wallets AS t ON "to" = t.wallet
      JOIN transfers_tokens USING (contract_address)
  ),
  transfers_with_price AS (
    SELECT
      ts,
      token_address_specific,
      CAST(token_amount AS DOUBLE) * price AS amount,
      token_address,
      CAST(token_amount AS DOUBLE) AS token_amount,
      reference,
      from_wallet,
      to_wallet,
      from_name,
      to_name
    FROM
      transfers_wallet_transfers
      LEFT JOIN transfers_prices ON transfers_wallet_transfers.token_address = transfers_prices.contract_address
      AND DATE_TRUNC('DAY', transfers_wallet_transfers.ts) = transfers_prices.period
  ),
  transfers_accounting AS (
    SELECT
      'TRANSFER' || CAST(RANK() OVER (
        ORDER BY
          ts NULLS FIRST,
          token_address_specific NULLS FIRST,
          CAST(token_amount AS DOUBLE) DESC
      ) AS VARCHAR) AS transaction,
      1 AS line,
      CASE
        WHEN token_address_specific = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 THEN 111001 /* USDC */
        WHEN token_address_specific = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 THEN 131004 /* steth */
        WHEN token_address_specific = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 131005 /* seth2 */
        WHEN token_address_specific = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121 /* WETH */
        ELSE 129 /* other cryptos */
      END AS account,
      'TRANSFER' AS ledger,
      ts,
      CAST(amount AS DOUBLE) AS amount,
      token_address,
      CAST(token_amount AS DOUBLE) AS token_amount,
      CAST(reference AS VARCHAR) AS reference,
      CAST(to_wallet AS VARCHAR) AS wallet
    FROM
      transfers_with_price
    UNION ALL
    SELECT
      'TRANSFER' || CAST(RANK() OVER (
        ORDER BY
          ts NULLS FIRST,
          token_address_specific NULLS FIRST,
          CAST(token_amount AS DOUBLE) DESC
      ) AS VARCHAR) AS transaction,
      2 AS line,
      CASE
        WHEN token_address_specific = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 THEN 111001 /* USDC */
        WHEN token_address_specific = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 THEN 131004 /* steth */
        WHEN token_address_specific = 0xfe2e637202056d30016725477c5da089ab0a043a THEN 131005 /* seth2 */
        WHEN token_address_specific = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 121 /* WETH */
        ELSE 129 /* other cryptos */
      END AS account,
      'TRANSFER' AS ledger,
      ts,
      - CAST(amount AS DOUBLE) AS amount,
      token_address,
      - CAST(token_amount AS DOUBLE) AS token_amount,
      CAST(reference AS VARCHAR) AS reference,
      CAST(from_wallet AS VARCHAR) AS wallet
    FROM
      transfers_with_price
  )
SELECT
  *
FROM
  transfers_accountingh_price
  )
SELECT
  *
FROM
  transfers_accounting