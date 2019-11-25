

//Rank for the actor by movie-count
----------DATA(kmdb.km_movie_actor)--------
actor_name,movie_name
a,m1
b,m2
c,m3
a,m4
b,m4
d,m5
e,m6
f,m2
g,m7
b,m8
g,m9
-----------------------

Output:
+----------+------+---------+-----------+------------+
|actor_name|mv_cnt|mv_cnt_rn|mv_cnt_rank|mv_cnt_drank|
+----------+------+---------+-----------+------------+
|b         |3     |1        |1          |1           |
|g         |2     |2        |2          |2           |
|a         |2     |3        |2          |2           |
|f         |1     |4        |4          |3           |
|e         |1     |5        |4          |3           |
|d         |1     |6        |4          |3           |
|c         |1     |7        |4          |3           |
+----------+------+---------+-----------+------------+


 WITH mv_counts AS (
  SELECT actor_name, count(1) AS mv_cnt
   FROM kmdb.km_movie_actor
    GROUP BY actor_name
 )
 ,act_rnks AS (
   SELECT actor_name,mv_cnt
    ,row_number() OVER (ORDER BY mv_cnt DESC) AS mv_cnt_rn
    ,rank() OVER (ORDER BY mv_cnt DESC) AS mv_cnt_rank
    ,dense_rank() OVER (ORDER BY mv_cnt DESC) AS mv_cnt_drank
      FROM mv_counts
 )
 SELECT * FROM act_rnks
  --WHERE mv_cnt_drank<=10
  ORDER BY mv_cnt_drank
   LIMIT 10
;
  
