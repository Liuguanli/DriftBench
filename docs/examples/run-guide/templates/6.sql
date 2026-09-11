-- Teaching adaptation of the Q6 shape with explicit date boundaries.
-- Runs on PostgreSQL and SQLite for the miniature demonstration.
-- This supplied template is not an official TPC-H compliance implementation.
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= ':1'
  AND l_shipdate < ':2'
  AND l_discount BETWEEN :3 - 0.01 AND :3 + 0.01
  AND l_quantity < :4;
