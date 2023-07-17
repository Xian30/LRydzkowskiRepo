--1. 
--1.1 Dataset covers data from 31-12-2019 to 14-12-2020
--1.2 There are 214 distinct values in countries_and_territory column and 212 distinct values in country_territory_columny
--    Assuming that Wallis_and_Futuna should have its own country_territory_code, there are 213 countries in the dataset. Otherwise, there are 212. 

SELECT DISTINCT 	
  countries_and_territories, 
  country_territory_code
FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide`


SELECT 
  MIN(covid.date) min_date,
  MAX(covid.date) max_date,
  COUNT(DISTINCT  countries_and_territories) countries_and_territories,
  COUNT(DISTINCT country_territory_code) country_territory_code,
FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid

--2. United States of America had the most deaths on a single day. There were 4928 deaths on the 16th of april 2020 
SELECT 
  covid.date,
  covid.daily_deaths,
  covid.countries_and_territories
FROM
`bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
ORDER BY 2 DESC

--or
SELECT
  date, 
  daily_deaths,
  countries,
  d_rank
FROM 
  (
    SELECT 
      covid.date date,
      covid.daily_deaths daily_deaths,
      covid.countries_and_territories countries,
      ROW_NUMBER() OVER(ORDER BY daily_deaths DESC) d_rank
    FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
    ORDER BY 4 ASC
  )
WHERE
  d_rank = 1

--3. United_States_of_America had the most deaths in August 2020. 
-- However, the number of deaths depends on whehter we sum daily deaths or substract the number of deaths at the beginning of august 2020 from the
-- number of deaths at the end of august 2020
-- In first case there were 30999 deaths. In second case there were 29755
-- Probably there were some corrections in data. That's why daily data do not sum up to cumulative data 
SELECT 
  covid.countries_and_territories,
  SUM(daily_deaths)
FROM
`bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
WHERE DATE >= '2020-08-01' AND DATE <= '2020-08-31'
GROUP BY covid.countries_and_territories
ORDER BY 2 DESC


SELECT
  date,
  countries_and_territories,
  deaths,
  LAG(deaths) OVER(PARTITION BY countries_and_territories ORDER BY covid.DATE) deaths_at_the_begin,
  deaths - (LAG(deaths) OVER(PARTITION BY countries_and_territories ORDER BY covid.DATE)) monthly_diff
FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
WHERE DATE IN ('2020-08-01','2020-08-31')
AND countries_and_territories = "United_States_of_America"
ORDER BY monthly_diff DESC


--4. There are 196 countries which have complete(i.e. one sample per day data) from at least 2020-04-01 to 2020-11-30

-- There should be 244 observations in the given period
SELECT DATE_DIFF(DATE('2020-11-30'), DATE('2020-04-01'), DAY)+1
FROM (select session_user())

SELECT COUNT(DISTINCT countries_and_territories)
FROM (
  SELECT
    countries_and_territories,
    COUNT(covid.DATE) num_observations
  FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
  WHERE date >= "2020-04-01" AND  date <= "2020-11-30" 
  GROUP BY 1
  HAVING num_observations = 244
  ORDER BY 2 DESC
  )

-- 5
-- the 7th of september 2020 was the deadliest defined as 'daily_deaths / sum of daily confirmed cases over last 14 days'
-- It refers to Ecuador
-- Neverthelles, this is a result of correction of daily_confirmed_cases so that the number was negative that day 

WITH full_obs_countries AS (
  SELECT DISTINCT countries_and_territories
  FROM (
    SELECT
      countries_and_territories,
      COUNT(covid.DATE) num_observations
    FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
    WHERE date >= "2020-04-01" AND  date <= "2020-11-30" 
    GROUP BY 1
    HAVING num_observations = 244
    ORDER BY 2 DESC
    )
)

SELECT 
  date,
  countries_and_territories,
  daily_confirmed_cases,
  daily_deaths,
  SUM(daily_confirmed_cases) OVER(PARTITION BY countries_and_territories ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) confirmed_cases_over_last_14_days,
  SAFE_DIVIDE(daily_deaths,(SUM(daily_confirmed_cases) OVER(PARTITION BY countries_and_territories ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW))) measure
FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
WHERE countries_and_territories in (SELECT countries_and_territories FROM full_obs_countries)
ORDER BY 6 DESC

-- check Ecuador data
SELECT 
  date,
  daily_confirmed_cases,
  daily_deaths,
  SUM(daily_confirmed_cases) OVER(PARTITION BY countries_and_territories ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) confirmed_cases_over_last_14_days,
  SAFE_DIVIDE(daily_deaths,(SUM(daily_confirmed_cases) OVER(PARTITION BY countries_and_territories ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW))) measure
FROM `bigquery-public-data.covid19_ecdc_eu.covid_19_geographic_distribution_worldwide` covid
WHERE date >= "2020-04-01" AND  date <= "2020-11-30" and
countries_and_territories = "Ecuador"
ORDER BY date