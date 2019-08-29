--Table used for test
CREATE EXTERNAL TABLE vivid.km_ext
(
 create_ts timestamp,
 id string
)
PARTITIONED BY (extract_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/vivid/stage/km_ext';

-----------------------------hive---------------------------------------------
----------------------- hive_params.hql START ----------------------------------
set mapreduce.job.queuename=general;
set hive.exec.dynamic.partition=true;  --default set to true in newer versions
set hive.exec.dynamic.partition.mode=nonstrict;

--Option#1
WITH
 rec as (SELECT current_timestamp() AS dt_ts, 'KM_07-23-2019_04_scr' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, '${hivevar:extract_date}'
    FROM rec a;
    
--Option#2
WITH
 --extr_dt AS (SELECT current_date AS dt)
 extr_dt AS (SELECT '${hivevar:extract_date}' AS dt)
 ,rec as (SELECT current_timestamp() AS dt_ts, 'KM_07-23-2019_04_scr' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, b.dt
    FROM rec a,extr_dt b;

---Query now

---hiveconf---
--set conf_extract_date=2019-07-23; --hard-coding hiveconf

SELECT *
 FROM vivid.km_ext
  WHERE extract_date>='${hiveconf:conf_extract_date}';

----------------------- hive_params.hql END ----------------------------------

----Execution
----beeline -u "jdbc:hive2://bdanode10.abc.com:10000/default;ssl=true;principal=hive/_HOST@BDA6.abc.COM?mapred.job.queue.name=general" \
 -f hive_params.hql \
 --hivevar extract_date=2019-07-23 \
 --hiveconf conf_extract_date=2019-07-23




------------------------------Impala-------------------------------------------------

----------------------- impala_params.impala START ----------------------------------

set REQUEST_POOL=general;

WITH
 rec as (SELECT cast(unix_timestamp(now()) AS timestamp) as dt_ts, 'KM_08-29-2019_02_impala' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, '${VAR:extract_date}'
    FROM rec a;
    
----------------------- impala_params.impala END ----------------------------------

---Execution:
impala-shell --ssl -i bda1node17 \
 -f impala_params.impala \
 --var=extract_date=2019-08-29

-------Other impala datetime functions
--from_unixtime(unix_timestamp(now()), 'yyyy/MM/dd HH:mm:ss') 
--cast(unix_timestamp(now()) AS timestamp) AS dt_ts --WORKS
--unix_timestamp('2019-08-29 16:37:33-07:00', 'yyyy-MM-dd HH:mm:ss-hh:mm')
--to_timestamp(string date, string pattern)

