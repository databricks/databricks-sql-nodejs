-- low_latency_tpch_sf1_q14.sql
---- QUERY: low_latency_tpch_sf1_q14
-- TPC-H v2.17 q14.sql a low latency query as per the criteria in the low latency initiative
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    main.tpch_sf1_delta.lineitem,
    main.tpch_sf1_delta.part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1995-09-01'
	and l_shipdate < date '1995-09-01' + interval '1' month

