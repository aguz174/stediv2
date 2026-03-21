CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-2`.`accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive' = 'TRUE',
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-1/accelerometer_landing/'
TBLPROPERTIES ('classification'='json');
