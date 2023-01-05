
---create table
CREATE EXTERNAL TABLE vivid.km_mrchnt (mrchnt_num STRING, name STRING, address STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
 LOCATION '/vivid/km/km_mrchnt';

msck repair table vivid.km_mrchnt;

---insert data
WITH
data AS (
 SELECT "1" AS mrchnt_num, 'ABC COMPANY, INC' AS name, '123 Main St' AS address
 UNION ALL
 SELECT "2" AS mrchnt_num, 'XYZ COMPANY INC.' AS name, '456' AS address
) INSERT INTO vivid.km_mrchnt SELECT * FROM data;

--INSERT OVERWRITE TABLE vivid.km_mrchnt SELECT * FROM data;

INSERT INTO vivid.km_mrchnt
 SELECT '3','WALMART - 2345', 'Alpha Lane';


----SELECT

#rows that have only numeric data
SELECT * FROM vivid.km_mrchnt WHERE cast(address as double) IS NOT NULL;
SELECT * FROM vivid.km_mrchnt WHERE address REGEXP '^[0-9]+$'

#extract numeric address# from address & remove
SELECT *, regexp_extract(address, '[0-9]+', 0) AS street_num, trim(regexp_extract(address, '[^0-9]+', 0)) as address_num_replaced FROM vivid.km_mrchnt;


