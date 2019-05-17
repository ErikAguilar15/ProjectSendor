SELECT DISTINCT l_receiptdate
FROM lineitem, orders, customer
WHERE	 o_custkey = c_custkey AND l_orderkey = o_orderkey AND
	o_orderdate>'1995-03-01' AND o_orderdate<'1996-03-01'
