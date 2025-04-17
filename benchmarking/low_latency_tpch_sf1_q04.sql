-- low_latency_tpch_sf1_q04.sql
---- QUERY: low_latency_tpch_sf1_q04
-- TPC-H v2.17 q4.sql a low latency query as per the criteria in the low latency initiative
select
	o_orderpriority,
	count(*) as order_count
from
    main.tpch_sf1_delta.orders
where
	o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
            main.tpch_sf1_delta.lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority

