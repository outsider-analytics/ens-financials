-- part of a query repo
-- query name: ENS-EnDAOment-Plugs
-- query link: https://dune.com/queries/3457789


WITH plugs_prices AS 
(
    SELECT DATE_TRUNC('day', minute) AS period
    , contract_address
    , price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
    AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    AND EXTRACT(HOUR FROM minute) = 23
    AND EXTRACT(MINUTE FROM minute) = 59
    AND DATE(minute) IN 
    (
        DATE('2023-01-26'),
        DATE('2023-01-30')
    )
), plugs_accounting_1 AS 
(
    SELECT
      'PLUG-1' AS transaction,
      1 AS line,
      131005 AS account,
      'PLUG' AS ledger,
      CAST('2023-01-26 20:30:35' AS TIMESTAMP) AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      0.005500054839813857 AS token_amount,
      '0x09757f9dbc1527e180f27e52ff930c7f3e8891d151f324cb649ba9a4031bf1b2' AS reference,
      '0x4f2083f5fbede34c2714affb3105539775f7fe64' AS wallet
    UNION ALL
    SELECT
      'PLUG-1' AS transaction,
      2 AS line,
      131005 AS account,
      'PLUG' AS ledger,
      CAST('2023-01-26 20:30:35' AS TIMESTAMP) AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      -0.005500054839813857 AS token_amount,
      '0x09757f9dbc1527e180f27e52ff930c7f3e8891d151f324cb649ba9a4031bf1b2' AS reference,
      'KPK' AS wallet
    UNION ALL
    SELECT
      'PLUG-2' AS transaction,
      1 AS line,
      121 AS account,
      'PLUG' AS ledger,
      CAST('2023-01-26 20:46:59' AS TIMESTAMP) AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      0.005 AS token_amount,
      '0x020d4cbfd482bc65a090eb9ba9d3e2cd203f042e07214877d13d8b348dc3589d' AS reference,
      '0x4f2083f5fbede34c2714affb3105539775f7fe64' AS wallet
    UNION ALL
    SELECT
      'PLUG-2' AS transaction,
      2 AS line,
      121 AS account,
      'PLUG' AS ledger,
      CAST('2023-01-26 20:46:59' AS TIMESTAMP) AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      -0.005 AS token_amount,
      '0x020d4cbfd482bc65a090eb9ba9d3e2cd203f042e07214877d13d8b348dc3589d' AS reference,
      'KPK' AS wallet
    UNION ALL
    SELECT
      'PLUG-3' AS transaction,
      1 AS line,
      121 AS account,
      'PLUG' AS ledger,
      CAST('2023-01-30 18:08:59' AS TIMESTAMP) AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      0.1 AS token_amount,
      '0xe95ce32b378a528a68f4383a78a8581aa4f3cd84774a38ed2bfa6aadde9074a6' AS reference,
      '0x4f2083f5fbede34c2714affb3105539775f7fe64' AS wallet
    UNION ALL
    SELECT
      'PLUG-3' AS transaction,
      2 AS line,
      121 AS account,
      'PLUG' AS ledger,
      CAST('2023-01-30 18:08:59' AS TIMESTAMP) AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      -0.1 AS token_amount,
      '0xe95ce32b378a528a68f4383a78a8581aa4f3cd84774a38ed2bfa6aadde9074a6' AS reference,
      'KPK' AS wallet
  ),
  plugs_accounting AS (
    SELECT
      transaction,
      line,
      account,
      ledger,
      FROM_UNIXTIME (TO_UNIXTIME (ts)) AS ts,
      CAST(token_amount AS DOUBLE) * price AS amount,
      token_address,
      CAST(token_amount AS DOUBLE) AS token_amount,
      reference,
      wallet
    FROM
      plugs_accounting_1 AS a
      JOIN plugs_prices AS p ON CAST(a.ts AS DATE) = p.period
    WHERE
      NOT transaction IS NULL
  )
SELECT
  *
FROM
  plugs_accountingaction IS NULL
  )
SELECT
  *
FROM
  plugs_accounting