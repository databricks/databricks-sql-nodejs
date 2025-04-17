-- large_results_0050mb.sql
---- QUERY: large_results_0050mb
-- Expanding the range of cs_sold_date_sk by 1 partition accounts for approximately 5MB result size
SELECT * FROM main.tpcds_sf100_delta.catalog_sales
WHERE cs_ship_mode_sk <= 14
  AND cs_sold_date_sk BETWEEN 2450815 AND 2450815 + 10;