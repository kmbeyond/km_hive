

----------Data
WITH
date_range AS (
  SELECT 'dt1' AS date_code, '2020-01-01' AS start_dt, '2020-01-31' AS end_dt
  UNION ALL
  SELECT 'dt2' AS date_code, '2020-02-01' AS start_dt, '2020-02-29' AS end_dt
  UNION ALL
  SELECT 'dt3' AS date_code, '2020-03-01' AS start_dt, '2020-03-31' AS end_dt
)
,my_data AS (
 SELECT '2020-01-15' AS txn_dt
 UNION ALL
 SELECT '2020-02-10' AS txn_dt
 UNION ALL
 SELECT '2020-05-12' AS txn_dt
)

Join both tables by the date range, output:
+-------------+--------------+--+
|  a.txn_dt   | b.date_code  |
+-------------+--------------+--+
| 2020-01-15  | dt1          |
| 2020-02-10  | dt2          |
| 2020-05-12  | NULL         |
+-------------+--------------+--+

----------------Impala

SELECT a.txn_dt, b.date_code
 FROM my_data a
  LEFT JOIN date_range b ON a.txn_dt BETWEEN b.start_dt AND b.end_dt      --works in Impala, but NOT in Hive
  --LEFT JOIN date_range b ON a.txn_dt>=b.start_dt AND a.txn_dt<=b.end_dt   --works in Impala, but NOT in Hive
;



---Hive


,dates_breakup AS (
  SELECT distinct date_code, date_add(rd.start_dt, dts.days) as active_dt
   FROM date_range rd LATERAL VIEW posexplode(split(space(datediff(rd.end_dt,rd.start_dt)),' ')) dts as days, x
)
SELECT a.txn_dt, b.date_code
 FROM my_data a
  LEFT JOIN dates_breakup b ON b.active_dt=a.txn_dt
;


