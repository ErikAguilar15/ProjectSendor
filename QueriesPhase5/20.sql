SELECT SUM(l_extendedprice * l_discount)
FROM lineitem
WHERE l_quantity < 12 AND l_returnflag = 'N' AND l_shipmode = 'AIR'
