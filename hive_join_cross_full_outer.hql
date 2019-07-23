------FULL OUTER JOIN (join by a column, return both table rows)

WITH
t1 AS (
 SELECT 1 AS id, 'abcd' AS v_t1
 UNION ALL
 SELECT 2 AS id, 'aaa' AS v_t1
)
,t2 AS (
 SELECT 2 AS id, 'bbb' AS v_t2
 UNION ALL
 SELECT 3 AS id, 'ccc' AS v_t2
)
 SELECT t1.id, t2.id, v_t1, v_t2
  FROM t1 FULL OUTER JOIN t2 ON t1.id=t2.id;


+--------+--------+--------+--------+--+
| t1.id  | t2.id  | v_t1  | v_t2  |
+--------+--------+--------+--------+--+
| 1      | NULL   | abcd   | NULL   |
| 2      | 2      | aaa    | bbb    |
| NULL   | 3      | NULL   | ccc    |


------CROSS JOIN (NO JOINED COLUMN: join every column from each table)

WITH
dt AS (
 SELECT '2019-01-01' AS max_dt
)
,t1 AS (
 SELECT 1 AS id, 'abcd' AS v_t1
 UNION ALL
 SELECT 2 AS id, 'aaa' AS v_t1
)
,t2 AS (
 SELECT 2 AS id, 'bbb' AS v_t2
 UNION ALL
 SELECT 3 AS id, 'ccc' AS v_t2
)
 SELECT t1.id, t2.id, v_t1, v_t2, dt.max_dt
  FROM t1, t2, dt


----3 tables: Both FULL OUTER & CROSS JOIN between 3 tables

--Scenario#1: First FULL OUTER then CROSS JOIN
WITH
dt AS (
 SELECT '2019-01-01' AS max_dt
)
,t1 AS (
 SELECT 1 AS id1, 'abcd' AS v_t1
 UNION ALL
 SELECT 2 AS id1, 'aaa' AS v_t1
)
,t2 AS (
 SELECT 2 AS id, 'bbb' AS v_t2
 UNION ALL
 SELECT 3 AS id, 'ccc' AS v_t2
)
 SELECT tj.id, tj.id, tj.v_t1, tj.v_t2, dt.max_dt
  FROM (SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id1=t2.id) tj, dt ;

--Scenario#2: 3 FULL OUTERS
WITH
t1 AS (
 SELECT 1 AS id, 'abcd' AS v_t1
 UNION ALL
 SELECT 2 AS id, 'aaa' AS v_t1
)
,t2 AS (
 SELECT 2 AS id, 'bbb' AS v_t2
 UNION ALL
 SELECT 3 AS id, 'ccc' AS v_t2
)
,t3 AS (
 SELECT 3 AS id, 'bbb' AS v_t3
 UNION ALL
 SELECT 4 AS id, 'ccc' AS v_t3
)
 SELECT id1, id2, t3.id AS id3,v_t1, v_t2,v_t3
  FROM (SELECT COALESCE(t1.id, t2.id) AS id, t1.id AS id1, t2.id AS id2,v_t1, v_t2 FROM t1 FULL OUTER JOIN t2 ON t1.id=t2.id ) tj
          FULL OUTER JOIN t3 ON t3.id=tj.id;
