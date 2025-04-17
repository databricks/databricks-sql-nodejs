-- low_latency_limit_0.sql
---- QUERY: low_latency_limit_0
-- limit 0 is useful to help track performance of processing table metadata, especially
-- for tables with large numbers of partitions.
SELECT * FROM main.tpch_sf1_delta.lineitem limit 0;
