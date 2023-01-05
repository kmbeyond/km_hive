


with
lookup AS (
 select 'MT' as TYPE, 'S' as LOOKUP, 'SINGLE' as LOOKUP_VAL
  union all
 select 'MT' as TYPE, 'M' as LOOKUP, 'MARRIED' as LOOKUP_VAL
  union all
 select 'MT' as TYPE, '' as LOOKUP, 'unspecified' as LOOKUP_VAL
)
,data as (
 select 'S' as marital_status
  union all
 select 'M' as marital_status
  union all
 select '' as marital_status
  union all
 select null as marital_status
)
select data.marital_status, lookup.LOOKUP_VAL
 FROM data
  LEFT JOIN lookup on lookup.TYPE='MT' AND trim(lookup.LOOKUP)=trim(data.marital_status)
 
=>
+----------------------+--------------------+
| data.marital_status  | lookup.lookup_val  |
+----------------------+--------------------+
| S                    | SINGLE             |
| M                    | MARRIED            |
|                      | unspecified        |
| NULL                 | NULL               |
+----------------------+--------------------+
