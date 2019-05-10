SELECT SUM(ps_supplycost), ps_suppkey
FROM partsupp
WHERE ps_suppkey > 50
GROUP BY ps_suppkey
