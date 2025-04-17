-- low_latency_tpch_sf1_q06.sql
---- QUERY: low_latency_tpch_sf1_q06
-- TPC-H v2.17 q6.sql a low latency query as per the criteria in the low latency initiative
select
	sum(l_extendedprice * l_discount) as revenue
from
    main.tpch_sf1_delta.lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between .06 - 0.01 and .06 + 0.01
	and l_quantity < 24

