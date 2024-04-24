SELECT *
FROM covid_df;

-- make staging table

CREATE TABLE covid_staging
LIKE covid_df;

INSERT INTO covid_staging
SELECT *
FROM covid_df;

SELECT *
FROM covid_staging;

-- duplicates 
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY continent, location, `date`, total_cases, new_cases, 
    total_deaths, new_deaths, positive_rate, stringency_index, population_density, 
    gdp_per_capita, life_expectancy, population) AS ranking
FROM covid_staging
ORDER BY ranking DESC
; -- no dups found

-- fix errors
SELECT DISTINCT location
FROM covid_staging;

SELECT 
	MIN(`date`),
    MAX(`date`),
    MIN(total_cases),
    MAX(total_cases),
    MIN(new_cases),
    MAX(new_cases),
    MIN(total_deaths),
    MAX(total_deaths),
    MIN(new_deaths),
    MAX(new_deaths),
    MIN(positive_rate),
    MAX(positive_rate),
    MIN(stringency_index),
    MAX(stringency_index),
    MIN(population_density),
    MAX(population_density),
    MIN(gdp_per_capita),
    MAX(gdp_per_capita),
    MIN(life_expectancy),
    MAX(life_expectancy),
    MIN(population),
    MAX(population)
FROM covid_staging;

-- little side quest to fix data types

SELECT `date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM covid_staging;

UPDATE covid_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE covid_staging
MODIFY COLUMN `date` DATE;

UPDATE covid_staging
SET total_cases = NULL
WHERE total_cases = '';

ALTER TABLE covid_staging
MODIFY COLUMN total_cases INT;

UPDATE covid_staging
SET total_deaths = NULL
WHERE total_deaths = '';

ALTER TABLE covid_staging
MODIFY COLUMN total_deaths INT;

UPDATE covid_staging
SET positive_rate = NULL
WHERE positive_rate = '';

ALTER TABLE covid_staging
MODIFY COLUMN positive_rate DOUBLE;

UPDATE covid_staging
SET stringency_index = NULL
WHERE stringency_index = '';

ALTER TABLE covid_staging
MODIFY COLUMN stringency_index DOUBLE;

UPDATE covid_staging
SET population_density = NULL
WHERE population_density = '';

ALTER TABLE covid_staging
MODIFY COLUMN population_density DOUBLE;

UPDATE covid_staging
SET gdp_per_capita = NULL
WHERE gdp_per_capita = '';

ALTER TABLE covid_staging
MODIFY COLUMN gdp_per_capita DOUBLE;

SELECT * FROM covid_staging;

SELECT 
	MIN(`date`),
    MAX(`date`),
    MIN(total_cases),
    MAX(total_cases),
    MIN(new_cases),
    MAX(new_cases),
    MIN(total_deaths),
    MAX(total_deaths),
    MIN(new_deaths),
    MAX(new_deaths),
    MIN(positive_rate),
    MAX(positive_rate),
    MIN(stringency_index),
    MAX(stringency_index),
    MIN(population_density),
    MAX(population_density),
    MIN(gdp_per_capita),
    MAX(gdp_per_capita),
    MIN(life_expectancy),
    MAX(life_expectancy),
    MIN(population),
    MAX(population)
FROM covid_staging;

-- no errors with maxs and mins so back to fixing values

SELECT 
	location, 
    population_density
FROM covid_staging
GROUP BY location, population_density
ORDER BY 1;

SELECT 
	location, 
    gdp_per_capita
FROM covid_staging
GROUP BY location, gdp_per_capita
ORDER BY 1;

SELECT 
	location, 
    life_expectancy
FROM covid_staging
GROUP BY location, life_expectancy
ORDER BY 1;

SELECT 
	location, 
    population
FROM covid_staging
GROUP BY location, population
ORDER BY 1;

SELECT *
FROM covid_staging
WHERE location LIKE '%part%'
ORDER BY `date`;


-- null values

-- handled above


-- remove rows and columns
SELECT
	SUM(CASE WHEN total_cases IS NULL THEN 1 ELSE 0 END) / COUNT(*),
    SUM(CASE WHEN total_deaths IS NULL THEN 1 ELSE 0 END) / COUNT(*),
    SUM(CASE WHEN positive_rate IS NULL THEN 1 ELSE 0 END) / COUNT(*),
    SUM(CASE WHEN stringency_index IS NULL THEN 1 ELSE 0 END) / COUNT(*),
    SUM(CASE WHEN population_density IS NULL THEN 1 ELSE 0 END) / COUNT(*),
    SUM(CASE WHEN population_density IS NULL THEN 1 ELSE 0 END) / COUNT(*)
FROM covid_staging;

SELECT
	location,
    stringency_index,
    COUNT(*)
FROM covid_staging
WHERE stringency_index IS NULL
GROUP BY location, stringency_index;

SELECT
	location,
    positive_rate,
    COUNT(*)
FROM covid_staging
WHERE positive_rate IS NULL
GROUP BY location, positive_rate;

SELECT
	`date`,
    location,
	stringency_index,
    positive_rate
FROM covid_staging
WHERE location = 'Aruba'
ORDER BY `date`;

-- both widespread and doesn't make sense to replace nulls

ALTER TABLE covid_staging
DROP COLUMN stringency_index;

ALTER TABLE covid_staging
DROP COLUMN positive_rate;

SELECT *
FROM covid_staging;

SELECT *
FROM covid_staging
WHERE total_cases IS NULL;

UPDATE covid_staging
SET total_cases = 0
WHERE total_cases IS NULL;

SELECT *
FROM covid_staging
WHERE total_deaths IS NULL;

UPDATE covid_staging
SET total_deaths = 0
WHERE total_deaths IS NULL;

-- EDA
-- try to emulate total cases and total deaths

SELECT 
	location,
    `date`,
    COUNT(*) AS num
FROM covid_staging
GROUP BY location, `date`
ORDER BY num DESC;

WITH my_total_cases_table AS
(
	SELECT 
		location,
		`date`,
		total_cases,
		new_cases,
		SUM(new_cases) OVER(PARTITION BY location ORDER BY `date`) AS my_total_cases
	FROM covid_staging
),
diff_and_row_num AS
(
	SELECT *,
		total_cases - my_total_cases AS total_cases_diff,
		ROW_NUMBER() OVER(partition by location ORDER BY `date`) AS row_num
	FROM my_total_cases_table
	WHERE total_cases - my_total_cases > 0
)
SELECT *
FROM diff_and_row_num
WHERE row_num = 1;

SELECT *
FROM covid_staging
WHERE location = 'Bahamas'
	AND `date` = '2020-08-29';
   

-- missing rows for 
	-- Bahamas 2020-08-30 37
    -- Belize 2021-02-27 9
    -- Canada 2022-06-18 23154
	-- Guatemala 2020-05-04 23
	-- Honduras 2021-01-12 699
    -- Jamaica 2022-06-04 386
    -- Panama 2023-05-07 591
	-- United States 2022-03-16 60969
    
INSERT INTO covid_staging (continent, location, `date`, total_cases, new_cases)
VALUES ('North America', 'Bahamas', '2020-08-30', 2057, 37), 
	('North America', 'Belize', '2021-02-27', 12280, 9), 
    ('North America', 'Canada', '2022-06-18', 3915567, 23154),
    ('North America', 'Guatemala', '2020-05-04', 688, 23),
    ('North America', 'Honduras', '2021-01-12', 127945, 699),
    ('North America', 'Jamaica', '2022-06-04', 138110, 386),
    ('North America', 'Panama', '2023-05-07', 1037324, 591),
    ('North America', 'United States', '2022-03-16', 78983223, 60969);
    
WITH my_total_cases_table AS
(
	SELECT 
		*,
		SUM(new_cases) OVER(PARTITION BY location ORDER BY `date`) AS my_total_cases
	FROM covid_staging
)
SELECT *,
	total_cases - my_total_cases AS total_cases_diff
FROM my_total_cases_table
WHERE total_cases - my_total_cases > 0;

SELECT *
FROM covid_staging;




SELECT COUNT(*)
FROM covid_staging;

-- 49482

SELECT 
	*,
    ((CASE WHEN population_density IS NULL THEN 1 ELSE 0 END) + 
    (CASE WHEN gdp_per_capita IS NULL THEN 1 ELSE 0 END)) AS null_count
FROM covid_staging
ORDER BY null_count DESC;

DELETE FROM covid_staging 
WHERE population_density IS NULL
	AND gdp_per_capita IS NULL;
    
SELECT COUNT(*)
FROM covid_staging;

-- 42056

SELECT *
FROM covid_staging;

-- drop columns with many nulls

-- did above 

