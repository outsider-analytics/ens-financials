-- part of a query repo
-- query name: ens_query_report
-- query link: https://dune.com/queries/3461449


SELECT
    *,
    count(*) OVER () as "Total Queries",
    count(CASE WHEN execution_state = 'QUERY_STATE_COMPLETED' THEN 1 END) OVER () AS "Total Successful Queries",
    count(CASE WHEN execution_state != 'QUERY_STATE_COMPLETED' THEN 1 END) OVER () AS "Total Failed Queries",
    SUBSTRING(CAST(max(completed_at) OVER () AS VARCHAR), 1, 10) as "Youngest Query",
    SUBSTRING(CAST(min(completed_at) OVER () AS VARCHAR), 1, 10) as "Oldest Query",
    SUBSTRING(CAST(min(report_date) OVER () AS VARCHAR), 1, 10) as "Report Date",
    max(execution_time) OVER () as "Longest Query"
FROM
    dune.outsider_analytics_team.dataset_ens_query_report
ORDER BY
    execution_state DESC, submitted_at, execution_time DESC