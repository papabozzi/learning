CREATE TABLE IF NOT EXISTS moonxyue.ratings (
    userId INT,
    movieId SMALLINT,
    rate TINYINT,
    ts INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::");
--total records: 1000209
LOAD DATA LOCAL INPATH '/data/hive/ratings.dat' OVERWRITE INTO TABLE moonxyue.ratings;

CREATE TABLE IF NOT EXISTS moonxyue.movies (
   movieId SMALLINT,
   movieName STRING,
   movieType STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::"); 
-- total records: 3883
LOAD DATA LOCAL INPATH '/data/hive/movies.dat' OVERWRITE INTO TABLE moonxyue.movies;

CREATE TABLE IF NOT EXISTS moonxyue.users (
    userId INT,
    sex CHAR(1),
    age TINYINT,
    occupation SMALLINT,
    zipCode CHAR(5)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::"); 
-- total records: 6040
LOAD DATA LOCAL INPATH '/data/hive/users.dat' OVERWRITE INTO TABLE moonxyue.users;
