


DROP TABLE kmdb.km_clms5 PURGE;

CREATE EXTERNAL TABLE IF NOT EXISTS kmdb.km_clms5
(
  c1 STRING 
 ,c2 STRING
)
partitioned by (extract_date string)
stored as PARQUET
LOCATION '/db/stage/km_clms5';

msck repair table kmdb.km_clms5;

DESC FORMATTED kmdb.km_clms5;


---------------------
----From existing table


CREATE EXTERNAL TABLE kmdb.km_tbl10
LIKE kmdb.km_clms5
LOCATION '/db/stage/km_tbl10'
;

---CTAS
CREATE EXTERNAL TABLE kmdb.new_test
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE 
AS select * from kmdb.clms5;



