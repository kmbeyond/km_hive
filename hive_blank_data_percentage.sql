

--Calculate percentage of blank tr_id data for each pin_flag

WITH
rec AS (SELECT pin_flag, (CASE WHEN tr_id='' THEN 1 else 0 END) as tr_flag_bool FROM km.table)
,tot_cnts AS (SELECT pin_flag, count(1) AS cnt, CAST( ((count(1)/(sum(count(1)) over ()))*100) AS DECIMAL(5,2)) AS percent FROM rec GROUP BY pin_flag)
,total AS (SELECT count(1) AS total FROM rec)
,total_metrics AS (SELECT pin_flag, a.cnt, total, percent FROM tot_cnts a,total b )
,blnk_id AS (SELECT pin_flag, sum(tr_flag_bool) AS blank_count FROM rec GROUP BY pin_flag)
--,blnk_id2 AS (SELECT pin_flag, count(1) AS cnt FROM rec WHERE tr_id='' GROUP BY pin_flag)
SELECT a.pin_flag, a.cnt, a.total, a.percent, b.blank_count, CAST((b.blank_count/a.total)*100 AS DECIMAL(10,2)) AS blank_pct
 FROM total_metrics a,blnk_id b WHERE a.pin_flag=b.pin_flag
;

