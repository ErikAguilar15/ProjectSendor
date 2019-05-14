SELECT c_name
FROM customer, orders
WHERE c_custkey = o_custkey
