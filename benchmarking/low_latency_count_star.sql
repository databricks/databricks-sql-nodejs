-- low_latency_count_star.sql
---- QUERY: low_latency_count_star
-- Description : Exercise Parquet stats optimization when evaluating count(*)
-- Target test case : Simple count(*) query.
SELECT count(*) FROM main.tpch_sf1_delta.lineitem;
