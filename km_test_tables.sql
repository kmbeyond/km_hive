
----------------------------- km_test tables -------------------------------------------------
DROP TABLE vivid.km_clms5 PURGE;
CREATE EXTERNAL TABLE IF NOT EXISTS vivid.km_clms5  (
 c1 STRING 
 ,c2 STRING
 ,c3 STRING
 ,c4 STRING
 ,c5 STRING
)
partitioned by (extract_date string)
stored as PARQUET
LOCATION '/vivid/stage/km_clms5';

msck repair table vivid.km_clms5;
DESC FORMATTED vivid.km_clms5;

---INSERT---------------
set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=general;

WITH final_data AS (
 SELECT "", "", ""
) --SELECT * FROM sample_data;
--dynamic
INSERT INTO TABLE vivid.km_clms5 PARTITION (extract_date)
 SELECT *, '','','2019-07-01_09_57' FROM final_data;

--static
INSERT INTO TABLE vivid.km_clms5 PARTITION (extract_date='2019-08-29_15_57')
 SELECT *, '','' FROM final_data;
 
SELECT * FROM vivid.km_clms5 WHERE extract_date='2019-07-01_09_57';
hdfs dfs -ls -t /vivid/stage/km_clms5/

--- columns10
DROP TABLE vivid.km_clms10 PURGE;
CREATE EXTERNAL TABLE IF NOT EXISTS vivid.km_clms10  (
 c1 STRING 
,c2 STRING
,c3 STRING
,c4 STRING
,c5 STRING
,c6 STRING
,c7 STRING
,c8 STRING
,c9 STRING
,c10 STRING
)
partitioned by (extract_date string)
stored as PARQUET
LOCATION '/vivid/stage/km_clms10';

msck repair table vivid.km_clms10;
DESC FORMATTED vivid.km_clms10;
