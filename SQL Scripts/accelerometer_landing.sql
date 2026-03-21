CREATE EXTERNAL TABLE IF NOT EXISTS `accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-1/accelerometer/accelerometer_landing/';