--------------------------Hive table with datetime/timestamp/millisec-----------------------------------------------------
NOTE: Impala does NOT support partition with timestamp

select current_date(), current_timestamp(), UNIX_TIMESTAMP(), from_unixtime(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.SSS');
=> 2019-07-14  | 2019-07-14 19:20:20.523  | 1563146420  | 2019-07-14 19:20:20.000 

select cast(current_timestamp() as date); => 2019-01-07
select to_date(current_timestamp()); => 2019-01-07


=>CAUTION: UNIX_TIMESTAMP(), from_unixtime() work at seconds level precision only

select FROM_UNIXTIME( 1546890411, 'YYYY-MM-dd HH:mm:ss.S' ); => 2019-01-07 02:46:51.0
select FROM_UNIXTIME( 1546890411666, 'YYYY-MM-dd HH:mm:ss.SSS' ); => 50989-01-02 12:01:06.000

*If data has milliseconds: Extract the milliseconds, append at the end then convert to timestamp
SELECT 1546890411666
 ,cast(concat( cast(from_unixtime(CAST(1546890411666/1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss') as String),
         '.', substr(cast(1546890411666 as String), 11, 3)) as timestamp);

+----------------+--------------------------+--+
|      _c0       |           _c1            |
+----------------+--------------------------+--+
| 1546890411666  | 2019-01-07 14:46:51.666  |
+----------------+--------------------------+--+

---------------------------datetime column operations--------------------
---------------------------(1) As timestamp column--------------------
CREATE EXTERNAL TABLE vivid.km_ex_ts
(
 `create_ts` timestamp,
 `id` string,
 `name` string
)
;
msck repair table vivid.km_ex_ts;
invalidate metadata vivid.km_ex_ts; --Impala

--Insert using Hive
WITH
data AS (
 SELECT '3' AS id, 'Jake' AS name
 UNION ALL
 SELECT '4' AS id, 'Adam' AS name
)
INSERT INTO vivid.km_ex_ts
 SELECT current_timestamp(), * FROM data;

--1) Insert using Spark & Hive current_timestamp() in Spark milliseconds
spark2-shell --queue general --conf spark.ui.port=4060
val df_km_ex_ts = Seq(("1", "Jim"), ("2","Joe")).toDF("id", "name")

df_km_ex_ts.createOrReplaceTempView("df_km_ex_ts")
spark.sql(s"INSERT INTO vivid.km_ex_ts SELECT current_timestamp(), * FROM df_km_ex_ts")
spark.sql(s"SELECT * FROM vivid.km_ex_ts").show(false)


--2) Insert using Spark datetime
var create_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S").format(new java.util.Date())

var df_km_ex_ts = Seq(("5", "Spark Insert")).toDF("id", "name")
df_km_ex_ts = df_km_ex_ts.withColumn("create_ts", lit(new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S").format(new java.util.Date()))).
 select("create_ts", "id", "name").show(false)
df_km_ex_ts.createOrReplaceTempView("df_km_ex_ts")
spark.sql(s"INSERT INTO vivid.km_ex_ts SELECT * FROM df_km_ex_ts")

df_km_ex_ts=spark.createDataFrame([("5", "Spark Insert")]).toDF("id", "name")
df_km_ex_ts.withColumn("create_ts", F.lit(datetime.now().strftime( '%Y-%m-%d %H:%M:%S' ) ) ) \
 .select("create_ts", "id", "name") \
 .write.insertInto("vivid.km_ex_ts")
 
--HQL
SELECT * from vivid.km_ex_ts
SELECT create_ts, to_date(create_ts) as create_dt, id, name FROM vivid.km_ex_ts
timestamp -> milliseconds (unix methods return seconds level precision ONLY)

SELECT create_ts, unix_timestamp(create_ts, 'yyyy-MM-dd hh:mm:ss.SSS') as create_ts_h, id, name FROM vivid.km_ex_ts;
timestamp -> String

SELECT create_ts, FROM_UNIXTIME(unix_timestamp(create_ts, 'yyyy-MM-dd hh:mm:ss.SSS'), 'yyyy-MM-dd hh:mm:ss') as create_ts_h, id, name FROM vivid.km_ex_ts;


--------------------(2) As milliseconds column------------------
CREATE EXTERNAL TABLE vivid.km_ex_ms
(
 `create_ts` bigint,
 `id` string,
 `name` string
)
;

--Insert data
INSERT INTO vivid.km_ex_ms
 SELECT UNIX_TIMESTAMP()*1000, '11' AS id, 'Phil' AS name;  --unix_timestamp() gives only till seconds level

select create_ts, from_unixtime(CAST(create_ts/1000 as BIGINT), 'yyyy-MM-dd hh:mm:ss.S') as create_ts_h, id, name from vivid.km_ex_ms;
select create_ts, CAST(cast(create_ts as float)/1000 AS timestamp) as create_ts_h, id, name from vivid.km_ex_ms;
select create_ts, CAST(create_ts/1000 AS timestamp) as create_ts_h, id, name from vivid.km_ex_ms;

--1) Insert using Spark milliseconds
val run_time_ms = System.currentTimeMillis()
spark.sql(s"insert into vivid.km_ex_ms select '$run_time_ms', '1000', 'Jim'")

val df_km_ex_ms = Seq(("1", "Jim"), ("2","Joe")).
 toDF("id", "name").
 withColumn("create_ts", lit(System.currentTimeMillis())).
 select("create_ts","id","name")
df_km_ex_ms.createOrReplaceTempView("df_km_ex_ms")
spark.sql(s"insert into vivid.km_ex_ms select * from df_km_ex_ms")

import org.apache.spark.sql.types.TimestampType
spark.sql("select * from vivid.km_ex_ms").withColumn("create_ts_2", (($"create_ts"/1000).cast("long")).cast(TimestampType)).show(false)


