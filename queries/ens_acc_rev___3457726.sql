-- part of a query repo
-- query name: ENS-Accounting-Revenues
-- query link: https://dune.com/queries/3457726


WITH ens_view_registrations AS (
    SELECT *, NULL as premium
    FROM ethereumnameservice_ethereum.ETHRegistrarController_1_evt_NameRegistered
    UNION 
    SELECT *, NULL as premium
    FROM ethereumnameservice_ethereum.ETHRegistrarController_2_evt_NameRegistered
    UNION 
    SELECT *, NULL as premium
    FROM ethereumnameservice_ethereum.ETHRegistrarController_3_evt_NameRegistered
    UNION 
    SELECT contract_address, evt_tx_hash, evt_index, evt_block_time, evt_block_number, baseCost + premium AS cost, expires, label, name, owner, premium
    FROM ethereumnameservice_ethereum.ETHRegistrarController_4_evt_NameRegistered
),
ens_view_renewals AS (
    SELECT *
    FROM ethereumnameservice_ethereum.ETHRegistrarController_1_evt_NameRenewed
    UNION 
    SELECT *
    FROM ethereumnameservice_ethereum.ETHRegistrarController_2_evt_NameRenewed
    UNION 
    SELECT *
    FROM ethereumnameservice_ethereum.ETHRegistrarController_3_evt_NameRenewed
    UNION 
    SELECT *
    FROM ethereumnameservice_ethereum.ETHRegistrarController_4_evt_NameRenewed
), revenues_periods AS (
    SELECT *
    FROM unnest(sequence(date('2018-01-01'), current_date, interval '1' day)) AS t(period)
),
/* Only ETH otherwise change the rest of the query */
revenues_tokens AS (
    SELECT contract_address, symbol, decimals, power(10, decimals) AS divisor
    FROM tokens.erc20
    WHERE blockchain = 'ethereum'
      AND contract_address IN (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 /* WETH */ )
),
revenues_prices AS (
    SELECT DATE_TRUNC('day', minute) AS period, contract_address, price
    FROM prices.usd
    INNER JOIN revenues_tokens USING (contract_address)
    WHERE blockchain = 'ethereum'
      AND EXTRACT(hour FROM minute) = 23
      AND EXTRACT(minute FROM minute) = 59
      AND NOT contract_address IN (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48) /* USDC */
      AND minute > CAST('2019-01-01' AS TIMESTAMP)
),
revenues_initial_events AS (
    SELECT
      contract_address AS wallet,
      evt_tx_hash,
      0 AS tpe,
      DATE_TRUNC('day', evt_block_time) AS period,
      name,
      cost,
      TO_UNIXTIME (evt_block_time) AS start_epoch,
      expires AS end_epoch,
      TO_UNIXTIME (evt_block_time) AS day_start_epoch,
      LEAST (
        cast(TO_UNIXTIME(DATE_TRUNC('day', evt_block_time) + INTERVAL '1' day) as uint256)
        ,
        expires
      ) AS day_end_epoch
    FROM
      ens_view_registrations
    WHERE
      evt_block_time > CAST('2019-01-01' AS TIMESTAMP)
    UNION ALL
    SELECT
      contract_address AS wallet,
      evt_tx_hash,
      1 AS tpe,
      DATE_TRUNC('day', evt_block_time) AS period,
      name,
      cost,
      TO_UNIXTIME(evt_block_time) AS start_epoch,
      expires AS end_epoch,
      TO_UNIXTIME(evt_block_time) AS day_start_epoch,
      LEAST (
        cast(TO_UNIXTIME(DATE_TRUNC('day', evt_block_time) + INTERVAL '1' day) as uint256),
        expires
      ) AS day_end_epoch
    FROM
      ens_view_renewals
    WHERE
      evt_block_time > CAST('2019-01-01' AS TIMESTAMP)
),
revenues_accruing AS (
    SELECT
      contract_address AS wallet,
      evt_tx_hash,
      0 AS tpe,
      period,
      name,
      cost,
      cast(TO_UNIXTIME (evt_block_time) as uint256) AS start_epoch,
      expires AS end_epoch,
      cast(GREATEST (
        TO_UNIXTIME (period),
        TO_UNIXTIME (evt_block_time)
      ) as uint256) AS day_start_epoch,
      LEAST (
        cast(TO_UNIXTIME (period + interval '1' day) as uint256),
        expires
      ) AS day_end_epoch
    FROM
      ens_view_registrations
      INNER JOIN revenues_periods ON evt_block_time < period + interval '1' day
      AND expires > cast(TO_UNIXTIME (period) as uint256)
    WHERE
      evt_block_time > CAST('2019-01-01' AS TIMESTAMP)
      AND period > evt_block_time /* Don't take the first day which is taken care in the above query */
    UNION ALL
    SELECT
      contract_address AS wallet,
      evt_tx_hash,
      1 AS tpe,
      period,
      name,
      cost,
      cast(TO_UNIXTIME (evt_block_time) as uint256) AS start_epoch,
      expires AS end_epoch,
      cast(GREATEST (
        TO_UNIXTIME (period),
        TO_UNIXTIME (evt_block_time)
      ) as uint256) AS day_start_epoch,
      LEAST (
        cast(TO_UNIXTIME (period + interval '1' day) as uint256),
        expires
      ) AS day_end_epoch
    FROM
      ens_view_renewals
      INNER JOIN revenues_periods ON evt_block_time < period + interval '1' day
      AND expires > cast(TO_UNIXTIME (period) as uint256)
    WHERE
      evt_block_time > CAST('2019-01-01' AS TIMESTAMP)
      AND period > evt_block_time /* Don't take the first day which is taken care in the above query */
),
revenues_initials_tx AS (
    SELECT
      wallet,
      tpe,
      period,
      SUM(
        (
          cast(cost as double) * (cast(day_end_epoch as double) - cast(day_start_epoch as double))  / (cast(end_epoch as double) - cast(start_epoch as double))
        )
      ) / POWER(10, 18) AS period_earned,
      cast(SUM(cost) as double) / POWER(10, 18) AS period_inflow,
      SUM(
        (
          cast(cost as double) - (
            cast(cost as double) * (cast(day_end_epoch as double) - cast(day_start_epoch as double))  / (cast(end_epoch as double) - cast(start_epoch as double))
          )
        )
      ) / POWER(10, 18) AS period_unearned
    FROM
      revenues_initial_events
    GROUP BY
      1,
      2,
      3
),
revenues_accruing_by_periods AS (
    SELECT  wallet, tpe, period,
        SUM((cast(cost as double) * (cast(day_end_epoch as double) - cast(day_start_epoch as double)) / 
            (cast(end_epoch as double) - cast(start_epoch as double))
        )) / POWER(10, 18) AS period_earned,
        SUM((cast(cost as double) * (cast(end_epoch as double) - cast(day_end_epoch as double)) / 
            (cast(end_epoch as double) - cast(start_epoch as double))
        )) / POWER(10, 18) AS period_remaining_to_earn
    FROM revenues_accruing
    WHERE period < CURRENT_DATE
    GROUP BY 1, 2, 3
),
revenues_accruing_lags as (
    select wallet, tpe, period,
        period_remaining_to_earn,
        lag(period_remaining_to_earn, 1) over (partition by wallet, tpe order by period asc) as lag_remaining_to_earn,
        price,
        lag(price, 1) over (partition by wallet, tpe order by period asc) as lag_price
    from revenues_accruing_by_periods
    left join revenues_prices using (period)
),
revenues_short_name_claims_revenue AS (
    SELECT
      block_time AS ts,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token_address,
      SUM(CAST(value AS DOUBLE)) / 1e18 AS token_amount,
      tx_hash AS reference,
      to AS wallet
    FROM
      ethereum.traces AS tr
    WHERE
      success = TRUE
      AND to = 0x911143d946ba5d467bfc476491fdb235fef4d667
      AND "from" = 0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749
      AND value > cast(0 as uint256)
      AND (
        NOT call_type IN ('delegatecall', 'callcode', 'staticcall')
        OR call_type IS NULL
      )
    GROUP BY
      1,
      4,
      5
  ),
  revenues_accounting AS (
    SELECT
      'REV-' || DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d') || '-' || CASE
        WHEN tpe = 0 THEN 'NEW'
        ELSE 'RENEW' END
        || '-' || CAST(rank() over (partition by period, tpe order by wallet) AS VARCHAR)
      AS transaction,
      1 AS line,
      121 AS account,
      'CASH' AS ledger,
      period,
      period_inflow * price AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      /*       '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_address, 
      period_earned as currency_amount, */
      AS token_address,
      period_inflow AS token_amount,
      NULL AS reference /* TODO (but would create too many rows?) */,
      CAST(wallet AS VARCHAR) AS wallet /* TODO */
    FROM
      revenues_initials_tx
      INNER JOIN revenues_prices USING (period)
    WHERE
      revenues_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    UNION ALL
    SELECT
      'REV-' || DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d') || '-' || CASE
        WHEN tpe = 0 THEN 'NEW'
        ELSE 'RENEW' END
        || '-' || CAST(rank() over (partition by period, tpe order by wallet) AS VARCHAR)
      AS transaction,
      2 AS line,
      21 AS account,
      'CASH' AS ledger,
      period,
      period_unearned * price AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      /*       '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_address, 
      period_earned as currency_amount, */
      AS token_address,
      period_unearned AS token_amount,
      NULL AS reference /* TODO (but would create too many rows?) */,
      CAST(wallet AS VARCHAR) AS wallet /* TODO */
    FROM
      revenues_initials_tx
      INNER JOIN revenues_prices USING (period)
    WHERE
      revenues_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    UNION ALL
    SELECT
      'REV-' || DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d') || '-' || CASE
        WHEN tpe = 0 THEN 'NEW'
        ELSE 'RENEW' END
        || '-' || CAST(rank() over (partition by period, tpe order by wallet) AS VARCHAR)
      AS transaction,
      3 AS line,
      CASE
        WHEN tpe = 0 THEN 3211 /* registration */
        WHEN tpe = 1 THEN 3212 /* renewal */
      END AS account,
      'CASH' AS ledger,
      period,
      period_earned * price AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      /*       '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_address, 
      period_earned as currency_amount, */
      AS token_address,
      period_earned AS token_amount,
      NULL AS reference /* TODO (but would create too many rows?) */,
      CAST(wallet AS VARCHAR) AS wallet /* TODO */
    FROM
      revenues_initials_tx
      INNER JOIN revenues_prices USING (period)
    WHERE
      revenues_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    UNION ALL
    SELECT
      'ACC-' || DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d') || '-' || CASE
        WHEN tpe = 0 THEN 'NEW'
        ELSE 'RENEW' END
        || '-' || CAST(rank() over (partition by period, tpe order by wallet) AS VARCHAR)
      AS transaction,
      1 AS line,
      CASE
        WHEN tpe = 0 THEN 3211 /* registration */
        WHEN tpe = 1 THEN 3212 /* renewal */
      END AS account,
      'REV' AS ledger,
      period,
      period_earned * price AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      /* '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency, 
      period_earned as currency_amount, */
      AS token_address,
      period_earned AS token_amount,
      NULL AS reference /* TODO (but would create too many rows?) */,
      CAST(wallet AS VARCHAR) AS wallet /* TODO */
    FROM
      revenues_accruing_by_periods
      INNER JOIN revenues_prices USING (period)
    WHERE
      revenues_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    UNION ALL
    SELECT
      'ACC-' || DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d') || '-' || CASE
        WHEN tpe = 0 THEN 'NEW'
        ELSE 'RENEW' END
        || '-' || CAST(rank() over (partition by period, tpe order by wallet) AS VARCHAR)
      AS transaction,
      2 AS line,
      CASE
        WHEN tpe = 0 THEN 21 /* registration */
        WHEN tpe = 1 THEN 21 /* renewal */
      END AS account,
      'REV' AS ledger,
      period,
      - period_earned * price AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      /*  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency, 
      -period_earned as currency_amount, */
      AS token_address,
      - period_earned AS token_amount,
      NULL AS reference /* TODO (but would create too many rows?) */,
      CAST(wallet AS VARCHAR) AS wallet /* TODO */
    FROM
      revenues_accruing_by_periods
      INNER JOIN revenues_prices USING (period)
    WHERE
      revenues_prices.contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    UNION ALL
    SELECT
      concat('SNC-', cast(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          reference
      ) as varchar)) AS transaction,
      1 AS line,
      121 AS account,
      'REV' AS ledger,
      DATE(ts) AS period,
      CAST(token_amount AS DOUBLE) * price AS amount,
      token_address,
      CAST(token_amount AS DOUBLE),
      CAST(reference AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      revenues_short_name_claims_revenue AS snc
      JOIN revenues_prices ON DATE(snc.ts) = revenues_prices.period
      AND snc.token_address = revenues_prices.contract_address
    UNION ALL
    SELECT
      concat('SNC-', cast(ROW_NUMBER() OVER (
        ORDER BY
          ts,
          reference
      ) as varchar)) AS transaction,
      2 AS line,
      3213 AS account,
      'REV' AS ledger,
      DATE(ts) AS period,
      CAST(token_amount AS DOUBLE) * price AS amount,
      token_address,
      CAST(token_amount AS DOUBLE),
      CAST(reference AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM
      revenues_short_name_claims_revenue AS snc
      JOIN revenues_prices ON DATE(snc.ts) = revenues_prices.period
      AND snc.token_address = revenues_prices.contract_address
      
    -- Mark to market of liabilities
    union all
    select 
      concat('ACC-M2M-', DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d'), '-', cast(ROW_NUMBER() OVER (
        PARTITION BY period ORDER BY cast(wallet as varchar)
      ) as varchar)) AS transaction,
      1 AS line,
      CASE
        WHEN tpe = 0 THEN 21 /* registration */
        WHEN tpe = 1 THEN 21 /* renewal */
      END AS account,
      'M2M-L' AS ledger,
      period,
      lag_remaining_to_earn * (price - lag_price) AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token_address,
      0,
      'qty ' || CAST(lag_remaining_to_earn AS VARCHAR) || ' price ' || CAST(price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM revenues_accruing_lags
    union all
    select 
      concat('ACC-M2M-', DATE_FORMAT(CAST(period AS TIMESTAMP), '%Y%m%d'), '-', cast(ROW_NUMBER() OVER (
        PARTITION BY period ORDER BY cast(wallet as varchar)
      ) as varchar)) AS transaction,
      2 AS line,
      3231 AS account,
      'M2M-L' AS ledger,
      period,
      -lag_remaining_to_earn * (price - lag_price) AS amount,
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token_address,
      0,
      'qty ' || CAST(lag_remaining_to_earn AS VARCHAR) || ' price ' || CAST(price AS VARCHAR) || ' prev price ' || CAST(lag_price AS VARCHAR) AS reference,
      CAST(wallet AS VARCHAR) AS wallet
    FROM revenues_accruing_lags
      
  )
SELECT
  *
FROM
  revenues_accountinglags
      
  )
SELECT
  *
FROM
  revenues_accounting