-- part of a query repo
-- query name: Income-Statement-ENS
-- query link: https://dune.com/queries/3454898


WITH entries AS (
    SELECT *
    FROM dune.outsider_analytics_team.result_ens_acc_main
),

items AS (
    SELECT
        1 AS rk,
        '&nbsp;Domain Registration' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3211%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        2 AS rk,
        '&nbsp;Domain Renewal' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3212%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        3 AS rk,
        '<b>Revenues</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '321%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        4 AS rk,
        '&nbsp;WG expenses' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3221%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        5 AS rk,
        '&nbsp;&nbsp;<i>Meta-gov</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(
            CASE WHEN CAST(account AS varchar) LIKE '3221001%' THEN amount END
        ) AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        6 AS rk,
        '&nbsp;&nbsp;<i>Ecosystem</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(
            CASE WHEN CAST(account AS varchar) LIKE '3221002%' THEN amount END
        ) AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        7 AS rk,
        '&nbsp;&nbsp;<i>Public Goods</i>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(
            CASE WHEN CAST(account AS varchar) LIKE '3221003%' THEN amount END
        ) AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        8 AS rk,
        '&nbsp;Grants' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3222%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        9 AS rk,
        '&nbsp;Suppliers' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3223%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        10 AS rk,
        '&nbsp;Other Expenses' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3229%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        11 AS rk,
        '<b>Operating Expenses</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '322%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        12 AS rk,
        '&nbsp;Currencies M2M' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3231%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        13 AS rk,
        '&nbsp;Investment P&L' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3232%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        14 AS rk,
        '&nbsp;Swaps P&L' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '3233%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        15 AS rk,
        '<b>Finacials Earnings</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '323%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
    UNION ALL
    SELECT
        16 AS rk,
        '<b>Total Earnings</b>' AS item,
        DATE_TRUNC('month', ts) AS month,
        SUM(CASE WHEN CAST(account AS varchar) LIKE '32%' THEN amount END)
            AS amount
    FROM entries
    GROUP BY DATE_TRUNC('month', ts)
),

pivot AS (
    SELECT
        rk,
        item,
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' YEAR
                    THEN amount
            END
        ) AS "12 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11' MONTH
                    THEN amount
            END
        ) AS "11 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '10' MONTH
                    THEN amount
            END
        ) AS "10 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '9' MONTH
                    THEN amount
            END
        ) AS "9 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '8' MONTH
                    THEN amount
            END
        ) AS "8 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '7' MONTH
                    THEN amount
            END
        ) AS "7 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6' MONTH
                    THEN amount
            END
        ) AS "6 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5' MONTH
                    THEN amount
            END
        ) AS "5 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '4' MONTH
                    THEN amount
            END
        ) AS "4 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3' MONTH
                    THEN amount
            END
        ) AS "3 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2' MONTH
                    THEN amount
            END
        ) AS "2 Months Ago",
        SUM(
            CASE
                WHEN
                    month
                    = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH
                    THEN amount
            END
        ) AS "1 Month Ago",
        SUM(
            CASE WHEN month = DATE_TRUNC('month', CURRENT_DATE) THEN amount END
        ) AS mtd,
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2019 THEN amount END)
            AS "2019",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2020 THEN amount END)
            AS "2020",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2021 THEN amount END)
            AS "2021",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2022 THEN amount END)
            AS "2022",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2023 THEN amount END)
            AS "2023",
        SUM(CASE WHEN EXTRACT(YEAR FROM month) = 2024 THEN amount END)
            AS "2024 YTD"
    FROM items
    GROUP BY rk, item
)

SELECT *
FROM pivot
ORDER BY rk ASCC