-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC clinicaltrial_2021 = spark.read.options(delimiter ='|', header = True).csv('/FileStore/tables/clinicaltrial_2021.csv/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021.createOrReplaceTempView('clinicaltrial_2021')

-- COMMAND ----------

SELECT * FROM clinicaltrial_2021

-- COMMAND ----------

SELECT DISTINCT COUNT ('Id') FROM clinicaltrial_2021

-- COMMAND ----------

SELECT SUM(frequency) as Total_Studies
FROM (SELECT Type, count(*) as frequency
FROM clinicaltrial_2021
WHERE Type != ''
GROUP BY Type)

-- COMMAND ----------

SELECT Type, count(*) as frequency
FROM clinicaltrial_2021
WHERE Type != ''
GROUP BY Type
ORDER BY frequency DESC
LIMIT 5;

-- COMMAND ----------

SELECT trim(subquery.Conditions) AS Conditions, count(*) as frequency
FROM  (SELECT explode(split(Conditions, ",")) as Conditions FROM clinicaltrial_2021) subquery
GROUP BY Conditions
ORDER BY frequency DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC pharma = spark.read.options(delimiter =',', header = True).csv('/FileStore/tables/pharma.csv/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC pharma.createOrReplaceTempView('pharma')

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS Frequency
FROM (
    SELECT c.Sponsor
    FROM Clinicaltrial_2021 c
    LEFT ANTI JOIN Pharma p
    ON c.Sponsor = p.Parent_Company
) 
GROUP BY Sponsor
ORDER BY Frequency DESC
LIMIT 10;

-- COMMAND ----------


SELECT month, Frequency
FROM(
    SELECT LEFT(Completion, 3) as month, COUNT(*) AS Frequency
    FROM Clinicaltrial_2021
    WHERE Status = 'Completed' AND RIGHT(Completion, 4) = '2021' AND Completion IS NOT NULL
    GROUP BY month
    ORDER BY 
        CASE 
            WHEN month = 'Jan' THEN 1 
            WHEN month = 'Feb' THEN 2 
            WHEN month = 'Mar' THEN 3 
            WHEN month = 'Apr' THEN 4 
            WHEN month = 'May' THEN 5 
            WHEN month = 'Jun' THEN 6 
            WHEN month = 'Jul' THEN 7 
            WHEN month = 'Aug' THEN 8 
            WHEN month = 'Sep' THEN 9 
            WHEN month = 'Oct' THEN 10 
            WHEN month = 'Nov' THEN 11 
            ELSE 12 
        END
)

-- COMMAND ----------


