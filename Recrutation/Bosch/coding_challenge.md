# Data Science Coding Challenge

This coding challenge consists of two parts:
* sql challenge
* python challenge

You can provide the results to us
* via checking the files into a GitLab or GitHub repository and sending us the link (preferred)
* by sending us the files via e-mail

Some Remarks:
* Provide both the code and the results to us. 
* We're not only interested in the "end result" but also in your thought processes, so make sure to leave some comments in the code explaining how you worked towards the result.
* Put a bit of effort into code structure and formatting, and also your repository structure if you decide to provide your results to us this way - we'll also have a look at that



## Part 1: Sql

For this challenge we make use of BigQuery and it's public "COVID-19 Cases by Country" dataset of the European Centre for Disease Prevention and Control. [See here how to query a public dataset with BigQuery](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console).

Most of the "Google SQL" on BigQuery follows the SQL standard - if in doubt you can check the [Google Query Syntax](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax).


### Questions

(1) What time frame does the dataset cover? how many countries does it contain?

(2) Which country had the most deaths due to Covid-19 on a single day, and which day was that?

(3) Which country had the most deaths due to Covid-19 in August 2020, and how many?

(4) How many countries have complete (i.e. one sample per day) data from at least April 2020 to (including) November 2020?

(5) On which day and in which country was Covid-19 deadliest, defined as ``daily_deaths / sum of daily confirmed cases over last 14 days``. Use only the subset of the countries found in Question 4 to answer this question. What do you observe?


## Part 2: Python

Please use Python to work on the following questions. You are free to choose suitable packages, e.g. pyspark, pandas, ...
Ideally, you create a Jupyter Notebook to share your results with us.


### Preparation

Get the hourly air temperature values from a German weather station near Reutlingen: [stundenwerte_TU_03278_20090401_20211231_hist.zip](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/stundenwerte_TU_03278_20090401_20211231_hist.zip)


### Questions

(1) The location of the weather station is contained within the file. Compute the air distance to the Bosch plant located at (48.49524, 9.13782) using a suitable formula.

Now read the file ``produkt_tu_stunde_20090401_20211231_03278.txt`` contained in the archive with Python and answer the following questions. The available columns are:
* ``STATIONS_ID``: not relevant
* ``MESS_DATUM``: date and time of measurement
* ``QN_9``: not relevant
* ``TT_TU`` air temperature in °C
* ``RF_TU`` relative humidity in %
* ``eor``: not relevant

Missing values in ``TT_TU`` and ``RF_TU`` are encoded as ``-999``.

(2) Describe the general properties of the dataset. This could be, among others:
* number of samples, missing values, ...
* measures of central tendency
* measures of variability (or spread)
* correlation measures

... that make sense in the given context.


(3) Compute the minimum, maximum and average temperature in January 2017.

(4) Assess the temperature development in the data. Can you find signs of climate change? Perform aggregations, visualizations to show your findings.