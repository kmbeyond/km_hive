

------km_hive_impala_params.sh-----
#!/bin/bash
#export PYTHON_EGG_CACHE=./myeggs

hive_url="jdbc:hive2://bda1.mk.com:10000/default;ssl=true;principal=hive/_HOST@BDA1.km.COM"
impala_d="bda1node17"
db_hive="vivid"
pool_name="general"
sReqId="aaaaa-11111-2222222"
s_process_dt="2020-01-20 12:48"


beeline -u "${hive_url}" -f hive_params.hql  \
       --hivevar "data_comm_schema=${db_hive}" --hivevar "pool_name=${pool_name}" --hivevar "process_dt=${s_process_dt}" --hivevar "ReqId=${sReqId}"

impala-shell --ssl -i ${impala_d} -f impala_params.impala \
      --var=data_comm_schema=${db_hive} --var=ReqId="${sReqId}" --var=process_dt="${s_process_dt}"




------- hive_params.hql----
set mapred.job.queue.name=${hivevar:pool_name};
SELECT current_date, '${hivevar:process_dt}' AS proc_dt, '${hivevar:ReqId}' as req_id;

--CAN ALSO USE directly: '${process_dt}'


------- impala_params.impala------
set REQUEST_POOL=general;

SELECT now() AS now, '${VAR:ReqId}' AS ReqId, '${VAR:process_dt}' AS Prc_dt
---------------------------------------------------------------------------------------------------------------------


--------------------------------------------with a table partition column
--Table used for test
CREATE EXTERNAL TABLE vivid.km_ext
(
 create_ts timestamp,
 id string
)
PARTITIONED BY (extract_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/vivid/stage/km_ext';

-----------------------------hive---------------------------------------------
----------------------- hive_params.hql START ----------------------------------
set mapreduce.job.queuename=general;
set hive.exec.dynamic.partition=true;  --default set to true in newer versions
set hive.exec.dynamic.partition.mode=nonstrict;

--Option#1: extract_date input arg as last column
WITH
 rec as (SELECT current_timestamp() AS dt_ts, 'KM_07-23-2019_04_scr' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, '${hivevar:extract_date}'
    FROM rec a;
    
--Option#2: If need to be used in a CTE or table (or for example, in condition) & join later
WITH
 --extr_dt AS (SELECT current_date AS dt)
 extr_dt AS (SELECT '${hivevar:extract_date}' AS dt)
 ,rec as (SELECT current_timestamp() AS dt_ts, 'KM_07-23-2019_04_scr' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, b.dt
    FROM rec a,extr_dt b;

---Query now

------------------------hiveconf------------
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

 --hivevar "start_dt_m5='${start_dt_m5}'"


------------------------------Impala-------------------------------------------------

----------------------- impala_params.impala START ----------------------------------

set REQUEST_POOL=general;

WITH
 rec as (SELECT cast(unix_timestamp(now()) AS timestamp) as dt_ts, 'KM_08-29-2019_02_impala' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, '${VAR:extract_date}'  --For current date: from_timestamp(now(), 'yyyy-MM-dd')
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

