


------FULL OUTER JOIN (join by a column, return both table rows)

WITH
t1 AS (
 SELECT 1 AS id, 'abcd' AS t1_v1
 UNION ALL
 SELECT 2 AS id, 'aaa' AS t1_v1
)
,t2 AS (
 SELECT 2 AS id, 'bbb' AS t2_v2
 UNION ALL
 SELECT 3 AS id, 'ccc' AS t2_v2
)
 SELECT t1.id, t2.id, t1_v1, t2_v2
  FROM t1 FULL OUTER JOIN t2 ON t1.id=t2.id;


------CROSS JOIN (NO JOINED COLUMN: join every column from each table)
WITH
t1 AS (
 SELECT 1 AS id, 'abcd' AS t1_v1
 UNION ALL
 SELECT 2 AS id, 'aaa' AS t1_v1
)
,t2 AS (
 SELECT 2 AS id, 'bbb' AS t2_v2
 UNION ALL
 SELECT 3 AS id, 'ccc' AS t2_v2
)
 SELECT t1.id, t2.id, t1_v1, t2_v2
  FROM t1, t2



