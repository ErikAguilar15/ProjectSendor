SELECT SUM(l_extendedprice * l_discount)
FROM lineitem
WHERE l_quantity < 10
