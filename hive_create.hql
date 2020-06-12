


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

---------------------With columns to be enclosed by double quote
CREATE EXTERNAL table kmdb.km_test_quotes
 (a string, b string, c string, d string)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
 WITH SERDEPROPERTIES (
   "separatorChar" = "|",
   "quoteChar"     = "\""
 )  
 LOCATION '/db/km/test_quotes/';
 
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



