-- part of a query repo
-- query name: Balance-Sheet-ENS
-- query link: https://dune.com/queries/3452083


WITH entries AS (
    SELECT *
    FROM dune.outsider_analytics_team.result_ens_acc_main
),

items AS (
    SELECT
        0 AS rk,
        '<b>Total Assets</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '1%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    -- Significant differences with stated balance sheet in reports need to sync up
    UNION ALL
    SELECT
        1 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;<i>Cash</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '111%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        2 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;<i>Money Markets</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '112%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        3 AS rk,
        '&nbsp;&nbsp;Cash & cash equivalents' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '11%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        4 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;ETH' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '12%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        5 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;ETH Investments' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '13%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        6 AS rk,
        '&nbsp;&nbsp;ETH Total' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '13%' OR CAST(account AS varchar) LIKE '12%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        7 AS rk,
        '<b>Total Liabilities</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '2%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        8 AS rk,
        '&nbsp;&nbsp;Unearned earnings' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '21%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        9 AS rk,
        '<b>Total Capital</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        10 AS rk,
        '&nbsp;&nbsp;Issued as payment' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '31%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        11 AS rk,
        '&nbsp;&nbsp;Retained earnings' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '32%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        12 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;<i>Operating revenues</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '321%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        13 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;<i>Operating expenses</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '322%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        14 AS rk,
        '&nbsp;&nbsp;&nbsp;&nbsp;<i>Financial earnings</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '323%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
),

balances AS (
    SELECT
        rk,
        item,
        month,
        SUM(amount) OVER (PARTITION BY item ORDER BY month ASC) AS balance
    FROM items
),

pivot AS (
    SELECT
        rk,
        item,
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH THEN balance END)
            AS "12 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11' MONTH THEN balance END)
            AS "11 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '10' MONTH THEN balance END)
            AS "10 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '9' MONTH THEN balance END)
            AS "9 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '8' MONTH THEN balance END)
            AS "8 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '7' MONTH THEN balance END)
            AS "7 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH THEN balance END)
            AS "6 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5' MONTH THEN balance END)
            AS "5 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '4' MONTH THEN balance END)
            AS "4 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3' MONTH THEN balance END)
            AS "3 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2' MONTH THEN balance END)
            AS "2 Months Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH THEN balance END)
            AS "1 Month Ago",
        SUM(CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) THEN balance END) 
            AS mtd,
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2019 AND EXTRACT(MONTH from month) = 12 THEN balance END)
            AS "2019",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2020 AND EXTRACT(MONTH from month) = 12 THEN balance END)
            AS "2020",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2021 AND EXTRACT(MONTH from month) = 12 THEN balance END)
            AS "2021",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2022 AND EXTRACT(MONTH from month) = 12 THEN balance END)
            AS "2022",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2023 AND EXTRACT(MONTH from month) = 12 THEN balance END)
            AS "2023",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2024 AND EXTRACT(MONTH from month) = EXTRACT(MONTH from CURRENT_DATE) THEN balance END)
            AS "2024 YTD"
    FROM balances
    GROUP BY rk, item
)

SELECT *
FROM pivot
ORDER BY rk ASC= 2020 THEN balance END)
            AS "2020",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2021 THEN balance END)
            AS "2021",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2022 THEN balance END)
            AS "2022",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2023 THEN balance END)
            AS "2023",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2024 THEN balance END)
            AS "2024 YTD"
    FROM balances
    GROUP BY rk, item
)

SELECT *
FROM pivot
ORDER BY rk ASC