


--using subquery
WITH
data AS (
 SELECT 'a' AS id
 UNION ALL SELECT 'a' AS id
 UNION ALL SELECT 'b' AS id
)
,id_totals AS (SELECT id, count(1) as id_cnt FROM data GROUP BY id)
,total AS (SELECT count(1) AS total FROM data)
SELECT a.id, a.id_cnt, b.total, (a.id_cnt/b.total)*100 AS pct
 FROM id_totals a ,total b
;
+-------+-----------+----------+--------------------+--+
| a.id  | a.id_cnt  | b.total  |        pct         |
+-------+-----------+----------+--------------------+--+
| a     | 2         | 3        | 66.66666666666666  |
| b     | 1         | 3        | 33.33333333333333  |
+-------+-----------+----------+--------------------+--+


--Using analytic/window function
WITH
data AS (
 SELECT 'a' AS id
 UNION ALL SELECT 'a' AS id
 UNION ALL SELECT 'b' AS id
)
SELECT id, count(1) AS cnt, ( count(1)/sum(count(1)) over () )*100 AS avg
 FROM data
 GROUP BY id
;
+-----+------+--------------------+--+
| id  | cnt  |        avg         |
+-----+------+--------------------+--+
| b   | 1    | 33.33333333333333  |
| a   | 2    | 66.66666666666666  |
+-----+------+--------------------+--+
