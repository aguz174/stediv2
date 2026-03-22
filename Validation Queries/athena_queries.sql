-- Landing Zones Validation Checks
SELECT COUNT(*) as customer_count FROM customer_landing;
SELECT COUNT(*) as accelerometer_count FROM accelerometer_landing;
SELECT COUNT(*) as step_trainer_count FROM steptrainer_landing;

SELECT COUNT(*) FROM stedi-2.customer_landing WHERE shareWithResearchAsOfDate IS NULL

-- Trusted Zones Validation Checks
SELECT COUNT(*) as consent_customers FROM customer_trusted;
SELECT COUNT(*) as trusted_accel FROM accelerometer_trusted;
SELECT COUNT(*) as trusted_step FROM steptrainer_trusted;

-- Curated Zones Validation Checks
SELECT COUNT(*) AS customer_curated_count FROM customer_curated;
SELECT COUNT(*) AS machinelearning_curated_count FROM machinelearning_curated;
