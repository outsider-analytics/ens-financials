-- part of a query repo
-- query name: Accounting-Main-ENS
-- query link: https://dune.com/queries/3457610


/* ENS Main Accounting query
 * refer to documentation https://docs.google.com/document/d/1xS4nXx1G0QCjFS-VdG5yVmVoMa5t1q9_dFZ9N4wGSJ8/edit
 * See also accounts dimension table => https://dune.com/queries/2181835
 * Example of query on top of this one => https://dune.com/queries/2181956
 *
 * version 2023-08-10
 * 2023-12-05: Adding Aave v3 ETH and Spark ETH
 * 2023-08-10: remove param so can be resued in other queries
 * 2023-08-07 fixed small error in curve strategy + dunesql version with mat views
 * 2023-07-26 fixing curve steth withdrawals
 * 2023-07-18: fixing aura weth withdrawals
 * 2023-06-02: Removing reference to ens.view_* and adding the code of those at the begining (adding registrar v4)
 */

WITH entries AS (
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_curve_steth_strat
    UNION ALL --putting disboi first to use ts(timestamp) instead of period(date). should fix queries to always use one or the other realistically.
    SELECT * FROM dune.outsider_analytics_team.result_ens_acc_rev
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_acc_exp
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_acc_assets_m2m --includes seth2_acc
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_acc_transfers
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_acc_swaps
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_comp_v3_weth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_aura_wsteth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_univ3_seth2_weth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_steth_strat
    UNION ALL
    SELECT * FROM query_3457789 --plugs_acc, does not need to materialize as it's mostly just manual entry
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_aura_reth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_dsr_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_reth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_comp_v3_usdc_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_ankreth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_ethx_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_comp_v3_weth_strat
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_aethweth_strat -- Aave v3 ETH
    UNION ALL
    SELECT * FROM dune.outsider_analytics_team.result_ens_end_spweth_strat -- Spark ETH
)

SELECT * FROM entries
ORDER BY ts, transaction, line
