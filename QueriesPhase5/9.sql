SELECT l_discount
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
