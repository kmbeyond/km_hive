


----------km_ext-----------------
DROP TABLE vivid.km_ext PURGE;

CREATE EXTERNAL TABLE vivid.km_ext
(
 create_ts timestamp,
 id string
)
PARTITIONED BY (extract_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/vivid/stage/km_ext'; --If omitted, auto-created location '/vivid/km_ext'

MSCK REPAIR TABLE vivid.km_ext;

--ALTER TABLE vivid.km_ext SET LOCATION '/vivid/stage/km_ext';

--INSERT: Using 'with insert'--
--static insert
WITH
 rec AS (select current_timestamp(), 'KM_07-23-2019' ) 
 INSERT INTO TABLE vivid.km_ext PARTITION (extract_date='2019-07-23') 
  SELECT * from rec;

--dynamic insert
set hive.exec.dynamic.partition=true;  --default set to true in newer versions 
set hive.exec.dynamic.partition.mode=nonstrict;
WITH
 extr_dt AS (SELECT current_date AS dt)
 ,rec as (SELECT current_timestamp() AS dt_ts, 'KM_07-23-2019_02' AS id) 
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, b.dt
    FROM rec a,extr_dt b;


ALTER TABLE vivid.km_ext_loc
 ADD PARTITION (extract_date='2000-01-01')
 location '/vivid/km_ext_loc/test/extract_date=2000-01-01';

ALTER TABLE vivid.km_ext_loc DROP PARTITION(extract_date='2000-01-01');
=>Deleted just partition, keeping the data


#Write using spark

var dataDF=Seq(("", "")).toDF("create_ts", "id")
dataDF=dataDF.unionAll( Seq((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new java.util.Date()), "3")).toDF("create_ts", "id") )
dataDF.filter($"create_ts" =!= "").show(false)

dataDF.
 coalesce(1).
 write.
 options(Map("delimiter" -> ",", "header"->"True")).
 csv("

------------------km_txns_prtn-------------------

CREATE EXTERNAL TABLE vivid.km_txns_prtn
(
txn_id     int,
txn_ts     bigint,
cust_id    int,
item_id    int,
qty        double,
disc_applied  boolean
)
PARTITIONED BY (extract_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

----read Spark
spark2-shell --queue=general
val origDF = spark.sql("select * from vivid.km_txns_prtn").filter($"txn_id" !== 0)
origDF.show()

spark.read.format("csv").option("delimiter", ",").load("/vivid/km_txns_prtn").show()
--_c0,..
spark.read.format("csv").option("delimiter", ",").load("/vivid/km_txns_prtn").toDF("A","B","C","D","E","F","G").show()
spark.read.format("csv").option("delimiter", ",").load("/vivid/km_txns_prtn/extract_date=2018-11-27").toDF("A","B","C","D","E","F").show() --One column less

val origDF = spark.
val respDF = spark.sql("select * from vivid.km_txns_prtn_resp")
respDF.filter($"txn_id" !== 0).show()

origDF.except(respDF).show()
----



------------Load from existing


---Static
INSERT INTO table vivid.km_txns_prtn partition (extract_date='2018-11-01')
 SELECT txn_id,txn_ts,cust_id,item_id,qty, disc_applied 
  FROM vivid.km_txns_raw;

#---Dynamic insert
set hive.exec.dynamic.partition.mode=nonstrict;

with rec as (select txn_id,txn_ts,cust_id,item_id,qty, disc_applied from vivid.km_txns_raw)
 INSERT INTO table vivid.km_txns_prtn partition (extract_date)
 select txn_id,txn_ts,cust_id,item_id,qty, disc_applied, current_date() from rec;

---Summary table
create table vivid.km_cust_summary
(
 cust_id int,
 cust_cnt bigint
)
partitioned by (extract_date string)
row format delimited fields terminated by ',';


WITH
cust_smry as(
 SELECT cust_id, extract_date, count(cust_id) AS cust_cnt
  FROM vivid.km_txns_prtn
   GROUP BY cust_id, extract_date
)
INSERT OVERWRITE table vivid.km_cust_summary partition (extract_date)
  select cust_id, cust_cnt, extract_date from cust_smry ;
  
