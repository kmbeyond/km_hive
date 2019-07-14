

SeLeCT * from (
SeLeCT tmp.*, 
 row_number() OVER ( PARTITION BY id ORDER BY score DESC) as row_nbr
 FROM (
   SeLeCT 'r1' as id, 100 as score union all
   SeLeCT 'r1' as id, 45 as score  union all
   SeLeCT 'r1' as id, 55 as score union all
   SeLeCT 'r2' as id, 66 as score  union all
   SeLeCT 'r2' as id, 77 as score union all
   SeLeCT 'r3' as id, 33 as score union all
   SeLeCT 'r3' as id, 88 as score 
  ) tmp 
) tmp2 where row_nbr=1;
