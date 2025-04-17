-- low_latency_tpch_sf1_q13.sql
---- QUERY: low_latency_tpch_sf1_q13
-- TPC-H v2.17 q13.sql a low latency query as per the criteria in the low latency initiative
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
            main.tpch_sf1_delta.customer left outer join main.tpch_sf1_delta.orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc

