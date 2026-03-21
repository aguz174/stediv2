-- Landing Zones Validation Checks
SELECT COUNT(*) as customer_count FROM customer_landing;
SELECT COUNT(*) as accelerometer_count FROM accelerometer_landing;
SELECT COUNT(*) as step_trainer_count FROM steptrainer_landing;

-- Trusted Zones Validation Checks
SELECT COUNT(*) as consenting_customers FROM customer_trusted;
SELECT COUNT(*) as trusted_accel FROM accelerometer_trusted;
SELECT COUNT(*) as trusted_step FROM steptrainer_trusted;

-- Curated Zones Validation Checks
SELECT COUNT(*) AS customer_curated_count FROM customer_curated;
SELECT COUNT(*) AS machine_learning_curated_count FROM machine_learning_curated;
