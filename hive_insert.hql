

#CREATE EXTERNAL TABLE kmdb.km_tbl1 (c1 STRING, p1 STRING, p2 STRING) LOCATION '/kmdb/common/km_tbl1';

WITH
data AS (
 SELECT "1" AS c1, 'p2_1' AS p2, 'p1_1' AS p1
 UNION ALL
 SELECT "2" AS c1, 'p2_2' AS p2, 'p1_2' as p1
)INSERT INTO kmdb.km_tbl1 SELECT * FROM data;


INSERT INTO kmdb.km_tbl2 PARTITION(p1,p2)
 SELECT c1,p1,p2 FROM kmdb.km_tbl1;
 

