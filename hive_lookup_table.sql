


with
lookup AS (
 select 'MT' as TYPE, 'S' as LOOKUP, 'SINGLE' as LOOKUP_VAL
  union all
 select 'MT' as TYPE, 'M' as LOOKUP, 'MARRIED' as LOOKUP_VAL
  union all
 select 'MT' as TYPE, '' as LOOKUP, 'unspecified' as LOOKUP_VAL
)
,data as (
 select 'S' as ms
  union all
 select 'M' as ms
  union all
 select '' as ms
)
select data.ms, lookup.LOOKUP_VAL
 FROM data join lookup on trim(lookup.LOOKUP)=trim(data.ms) AND lookup.TYPE='MT'
 
