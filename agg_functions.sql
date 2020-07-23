
--------test data:----
WITH
test AS (
 SELECT 1 AS id
 UNION ALL SELECT 1 AS id
 UNION ALL SELECT 2 AS id
)

----------Add/group------------

SELECT id, count(1) AS cnt FROM test
 GROUP BY id

--Using analytic/window function
,cnts AS (
 SELECT id, count(1) over (partition by id) AS cnt FROM test
)SELECT distinct * FROM cnts
;

-------------Calculate avg/percentage-----
--Using analytic/window function

SELECT id, count(1) AS cnt, (count(1)/(sum(count(1)) over ()))*100 AS avg
 FROM test
 GROUP BY id
;

--Using subquery

,id_totals AS (SELECT id, count(1) as id_cnt FROM test GROUP BY id)
,total AS (SELECT count(1) AS total FROM test)
SELECT a.id, a.id_cnt, b.total, (a.id_cnt/b.total)*100 AS pct
 FROM id_totals a ,total b
;
