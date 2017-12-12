CREATE TABLE movies
(movieId INT, title STRING, genres STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"',
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

CREATE TABLE tags
(userId INT, movieId INT, tag STRING, `timestamp` TIMESTAMP)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"',
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

CREATE TABLE ratings
(userId INT, movieId INT, rating DOUBLE, `timestamp` TIMESTAMP)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"',
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

CREATE TABLE genome_scores
(movieId INT, tagId INT, relevance DOUBLE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"',
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

CREATE TABLE genome_tags
(tagId INT, tag STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"',
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;
