------Dates between 2 dates / date ranges


--Only one date range
WITH
date_max_min AS(
  SELECT '2019-07-01' AS min_start_date, '2019-07-10' AS max_end_date
) --SELECT posexplode(split(space(datediff(max_end_date, min_start_date)),' ')) FROM date_max_min
SELECT distinct date_add(min_start_date, v.pos) AS dt 
 FROM date_max_min, ( SELECT posexplode(split(space(datediff(max_end_date, min_start_date)),' ')) FROM date_max_min ) v
;


-----Particular dates between different date ranges
WITH
chains_req_dates AS (
  SELECT 'CHN01' AS chain_id, '2019-07-01' AS start_date, '2019-07-10' AS end_date
  UNION ALL
  SELECT 'CHN01' AS chain_id, '2019-01-01' AS start_date, '2019-01-15' AS end_date
)
SELECT distinct date_add(rd.start_date, dts.days) as dt
  FROM chains_req_dates rd LATERAL VIEW posexplode(split(space(datediff(rd.end_date,rd.start_date)),' ')) dts as days, x
;
