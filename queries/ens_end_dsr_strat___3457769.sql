-- part of a query repo
-- query name: ENS-EnDAOment-DSR-Strategy
-- query link: https://dune.com/queries/3457769


WITH wallet_list AS 
(
    SELECT 0x4f2083f5fbede34c2714affb3105539775f7fe64 AS wallet /* endaoment addy */
), dsr_movements AS
(
    SELECT move.call_tx_hash AS hash
    , move.call_block_time AS ts
    , move.call_trace_address AS trace
    , COALESCE(dsr_mgr.dst, move.src) AS usr
    , CAST(move.rad AS INT256) AS rad
    , CAST(NULL AS INT256) AS rate
    FROM maker_ethereum.VAT_call_move move
    LEFT JOIN
    (SELECT contract_address, call_tx_hash, dst FROM maker_ethereum.dsrmanager_call_join WHERE call_success GROUP BY 1,2,3) dsr_mgr
    ON move.src = dsr_mgr.contract_address
    AND move.call_tx_hash = dsr_mgr.call_tx_hash
    WHERE move.call_success
    AND move.dst = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
    
    UNION ALL
    
    SELECT move.call_tx_hash AS hash
    , move.call_block_time AS ts
    , move.call_trace_address AS trace
    , COALESCE(dsr_mgr.dst, move.dst) AS usr
    , -CAST(move.rad AS INT256) AS rad
    , CAST(NULL AS INT256) AS rate
    FROM maker_ethereum.VAT_call_move move
    LEFT JOIN
    (SELECT contract_address, call_tx_hash, dst FROM maker_ethereum.dsrmanager_call_exit WHERE call_success GROUP BY 1,2,3) dsr_mgr
    ON move.dst = dsr_mgr.contract_address
    AND move.call_tx_hash = dsr_mgr.call_tx_hash
    WHERE call_success
    AND src = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7

    UNION ALL
    
    SELECT call_tx_hash AS hash
    , call_block_time AS ts
    , call_trace_address AS trace
    , NULL AS usr
    , CAST(NULL AS INT256) AS rad
    , CAST(output_tmp AS INT256) AS rate
    FROM maker_ethereum.pot_call_drip
    WHERE call_success
    AND call_block_time >= CAST ('2019-11-18 16:07' AS TIMESTAMP) --earliest entry into dsr
    
    UNION ALL
    
    SELECT CAST (NULL AS VARBINARY) AS hash
    , CAST(dt AS TIMESTAMP) - INTERVAL '1' SECOND AS ts
    , NULL AS trace--CAST (NULL AS ARRAY(BIGINT)) AS trace
    , NULL AS usr
    , CAST(NULL AS INT256) AS rad
    , CAST(NULL AS INT256) AS rate
    FROM
    UNNEST(SEQUENCE(DATE('2019-11-19'), CURRENT_DATE + INTERVAL '1' DAY, INTERVAL '1' DAY)) AS t(dt)
), rates_filled AS
(
    SELECT *
    , MAX(rate) OVER (PARTITION BY rate_grp) AS rate_filled
    FROM
    (
        SELECT *
        , SUM(CASE WHEN rate IS NOT NULL THEN 1 ELSE 0 END) OVER (ORDER BY ts, trace) AS rate_grp
        FROM dsr_movements
    ) sub
), piedai AS 
(
  SELECT rates_filled.hash
  , rates_filled.ts
  , rates_filled.trace
  , rates_filled.usr
  , rates_filled.rad/1e45 AS dai
  , rates_filled.rate_filled AS rate
  , rates_filled.rad / rates_filled.rate_filled / 1e18 AS pie
  FROM rates_filled
), dsr_movements2 AS
(
    SELECT * FROM piedai
    WHERE usr IS NOT NULL
    AND usr IN (SELECT wallet FROM wallet_list)
), min_ts AS
(
    SELECT usr
    , MIN(ts) AS min_ts
    FROM dsr_movements2
    GROUP BY 1
), rates AS
(
    SELECT piedai.hash
    , piedai.ts
    , piedai.trace
    , min_ts.usr
    , piedai.dai
    , piedai.rate
    , piedai.pie 
    FROM piedai
    JOIN min_ts
    ON piedai.ts > min_ts.min_ts
    WHERE piedai.usr IS NULL
), unioned AS
(
    SELECT * FROM dsr_movements2
    UNION ALL
    SELECT * FROM rates
), cum_sum AS
(
    SELECT *
    , SUM(pie) OVER(PARTITION BY usr ORDER BY ts, trace) AS cum_pie
    , rate - LAG(rate) OVER(PARTITION BY usr ORDER BY ts, trace) AS delta_rate
    FROM unioned
), dsr_accounting AS 
(
    SELECT 'ENTER-EXIT-DSR-' || CAST(ROW_NUMBER() OVER (PARTITION BY usr ORDER BY ts, dai) AS VARCHAR) AS transaction
    , 1 AS line
    , 112003 AS account --increase in money market assets
    , 'P&L' AS ledger
    , ts
    , dai AS amount
    , 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS token_address
    , dai AS token_amount
    , CAST(hash AS VARCHAR) AS reference
    , CAST(usr AS VARCHAR) AS wallet
    FROM cum_sum
    WHERE dai IS NOT NULL

    UNION ALL

    SELECT 'ENTER-EXIT-DSR-' || CAST(ROW_NUMBER() OVER (PARTITION BY usr ORDER BY ts, dai) AS VARCHAR) AS transaction
    , 2 AS line
    , 111002 AS account --decrease in cash assets
    , 'P&L' AS ledger
    , ts
    , -dai AS amount
    , 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS token_address
    , -dai AS token_amount
    , CAST(hash AS VARCHAR) AS reference
    , CAST(usr AS VARCHAR) AS wallet
    FROM cum_sum
    WHERE dai IS NOT NULL
    
    UNION ALL
    
    SELECT 'ACCRUAL-DSR-' || CAST(ROW_NUMBER() OVER (PARTITION BY usr ORDER BY ts, cum_pie * delta_rate/1e27) AS VARCHAR) AS transaction
    , 1 AS line
    , 112003 AS account --increase in money market assets
    , 'P&L' AS ledger
    , ts
    , cum_pie * delta_rate/1e27 AS amount
    , 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS token_address
    , cum_pie * delta_rate/1e27 AS token_amount
    , CAST(hash AS VARCHAR) AS reference
    , CAST(usr AS VARCHAR) AS wallet
    FROM cum_sum
    WHERE COALESCE(cum_pie * delta_rate/1e27, 0) <> 0
    
    UNION ALL

    SELECT 'ACCRUAL-DSR-' || CAST(ROW_NUMBER() OVER (PARTITION BY usr ORDER BY ts, cum_pie * delta_rate/1e27) AS VARCHAR) AS transaction
    , 2 AS line
    , 32321003 AS account --increase in money market assets
    , 'P&L' AS ledger
    , ts
    , cum_pie * delta_rate/1e27 AS amount
    , 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 AS token_address
    , cum_pie * delta_rate/1e27 AS token_amount
    , CAST(hash AS VARCHAR) AS reference
    , CAST(usr AS VARCHAR) AS wallet
    FROM cum_sum
    WHERE COALESCE(cum_pie * delta_rate/1e27, 0) <> 0
)
SELECT * FROM dsr_accounting