
-- create external table which contain data
create external table population_csv (
firstname STRING,
gender STRING,
origin string,
version DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
location '/user/olementec/prenoms';

--create table stored as ORC file with gender and origin as table
create table population_clean(
firstname STRING,
gender ARRAY<STRING>,
origin ARRAY<STRING>,
version DOUBLE)
stored as orc tblproperties ("orc.compress"="NONE");

----

-- insert content of the table
INSERT INTO TABLE population_clean
SELECT firstname firstname,
split(gender, ",") gender,
split(origin, ",") origin,
version version
FROM population_csv;
---


-- count redondancy of origin
SELECT origin, COUNT(origin) quantity_origin
FROM
(
SELECT explode(origin) origin
FROM population_clean
) a
group by origin;

-- count first name by origin (same as before but respect literaly the question count firstname by origin)
-- Count first name by origin
SELECT origin, COUNT(firstname) nb_origin
FROM
(
SELECT explode(origin) origin, firstname
FROM population_clean
) a
group by origin;

-- Count number of first name by number of origin (how many first name has x origins ? For x = 1,2,3...)

SELECT nb_origin, sum(1)
FROM
(
SELECT firstname, size(origin) nb_origin
FROM population_clean
) a
GROUP BY nb_origin

-- Proportion (in%) of male or female
SELECT sum(if(gender='f',1,0))/COUNT(*) female_proportion, sum(if(gender='m',1,0))/COUNT(*) male_proportion
FROM
(
SELECT explode(gender) gender
FROM population_clean
) nb_gender






