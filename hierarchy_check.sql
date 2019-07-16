


WITH
grp1 AS (
 SELECT distinct state, censustract AS c2, concat('C3-', censusblock) AS c3
  FROM kmtable
   WHERE 1=1
   --GROUP BY state,countyfips,zipcode
    --HAVING count(distinct zipcode)>1
),c3_dist AS (SELECT count(distinct c3) AS total_c3 FROM grp1)
,rev_grp1 AS (
 SELECT state,c3,c3_dist.total_c3, count(1) cnt, concat_ws('*', collect_set(c2)) as collected
   FROM grp1,c3_dist
    GROUP BY state,c3,total_c3
     HAVING count(1)>1
  --ORDER BY state,c3 desc
)
 SELECT count(1) AS cnt, c3_dist.total_c3
  FROM rev_grp1,c3_dist GROUP BY c3_dist.total_c3 LIMIT 50
;

