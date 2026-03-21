# stediv2
Udacity STEDI Human Balance Project for D609

​Project Overview

​This project involves building an ETL pipeline for a healthcare sensor product called STEDI. The goal is to process data from three different sources—customer records, accelerometer sensors, and step trainer sensors—to create a curated dataset for machine learning.

​Data Lake Architecture

​1. Landing Zone
​Source: Raw JSON data uploaded to S3.
​Tables: customer_landing, accelerometer_landing, step_trainer_landing.
​Status: Contains "raw" data, including non-consenting users and mismatched serial numbers.

​2. Trusted Zone
​Customer Trusted: Filtered to include only users who agreed to share data for research (shareWithResearchAsOfDate is not null).
​Accelerometer Trusted: An inner join between accelerometer_landing and customer_trusted to ensure data only exists for consenting users.
​Step Trainer Trusted: An inner join between step_trainer_landing and customer_curated to filter for valid device serial numbers.

​3. Curated Zone
​Customer Curated: A "master" filter created by joining customer_trusted with accelerometer_trusted.
​Machine Learning Curated: The final output. An inner join between step_trainer_trusted and accelerometer_trusted based on matching timestamps (sensorReadingTime = timestamp).

​Validation was performed using Athena SQL queries to verify row counts across all zones. 
