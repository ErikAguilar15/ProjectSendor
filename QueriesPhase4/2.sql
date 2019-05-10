SELECT SUM(c_acctbal), c_name
FROM customer
WHERE c_acctbal > 1.0
GROUP BY c_name
