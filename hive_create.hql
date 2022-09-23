


-------vivid.km_mrchnt
CREATE EXTERNAL TABLE vivid.km_mrchnt (mrchnt_num STRING, name STRING, address STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/vivid/km/km_mrchnt';
msck repair table vivid.km_mrchnt;

WITH
data AS (
 SELECT "1" AS mrchnt_num, 'ABC COMPANY, INC' AS name, '123 Main St' AS address
 UNION ALL
 SELECT "2" AS mrchnt_num, 'XYZ COMPANY INC.' AS name, '456' AS address
) INSERT INTO vivid.km_mrchnt SELECT * FROM data;

--INSERT OVERWRITE TABLE vivid.km_mrchnt SELECT * FROM data;

INSERT INTO vivid.km_mrchnt SELECT '3','WALMART - 2345', 'Alpha Lane';

10|Walmart|888 Main St
11|Acme Inc.|111 Main Ct

SELECT * FROM vivid.km_mrchnt WHERE cast(address as double) IS NULL;
SELECT * FROM vivid.km_mrchnt WHERE address REGEXP '^[0-9]+$'
SELECT *, regexp_extract(address, '[0-9]+', 0) AS street_num, trim(regexp_extract(address, '[^0-9]+', 0)) as address_num_replaced FROM vivid.km_mrchnt;

---------vivid.km_tran
CREATE EXTERNAL TABLE vivid.km_tran (tran_num STRING, tran_date STRING, mrchnt_num STRING, prod_code STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/vivid/km/km_tran';
msck repair table vivid.km_tran;

WITH
data AS (
 SELECT "1000" AS tran_num, '2022-01-01 12:34:23' AS tran_date, '2' AS mrchnt_num, '4444444' AS prod_code
 UNION ALL
 SELECT "1001" AS tran_num, '2022-09-21 09:55:56' AS tran_date, '3' AS mrchnt_num, '6666666' AS prod_code
) INSERT INTO vivid.km_tran SELECT * FROM data;



------------parquet
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



