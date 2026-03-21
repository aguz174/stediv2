CREATE EXTERNAL TABLE IF NOT EXISTS `steptrainer_landing` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-1/steptrainer/steptrainer_landing/';