
-----------------impala----------------
WITH
req1_impala AS
(
 select 'R1' AS req_id, 'Resolved' AS status
 UNION ALL
 select 'R2', 'In_progress'
) SELECT * FROM req1;



--------------Hive------------------------
WITH
req1_hive AS
(
 select 'R1', 'Resolved'
 UNION ALL
 select 'R2', 'In_progress'
)
req1 AS (
  SELECT `_c0` AS req_id, `_c1` AS status
   FROM req1_hive
)
SELECT * FROM req1;

