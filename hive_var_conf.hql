
----------------------- hive_params.hql START ----------------------------------
set mapreduce.job.queuename=harmonization;
set hive.exec.dynamic.partition=true;  --default set to true in newer versions
set hive.exec.dynamic.partition.mode=nonstrict;


WITH
 --extr_dt AS (SELECT current_date AS dt)
 extr_dt AS (SELECT '${hivevar:extract_date}' AS dt)
 ,rec as (SELECT current_timestamp() AS dt_ts, 'KM_07-23-2019_04_scr' AS id)
 INSERT INTO TABLE vivid.km_ext PARTITION(extract_date)
  SELECT a.dt_ts, a.id, b.dt
    FROM rec a,extr_dt b;


---hiveconf---
set conf_extract_date=2019-07-23;

SELECT *
 FROM vivid.km_ext
  WHERE extract_date>='${hiveconf:conf_extract_date}';

----------------------- hive_params.hql START ----------------------------------

----Execution
----beeline -u "jdbc:hive2://bda6node10.infoftps.com:10000/default;ssl=true;principal=hive/_HOST@BDA6.INFOFTPS.COM?mapred.job.queue.name=general" -f hive_params.hql --hivevar extract_date=2019-07-23



