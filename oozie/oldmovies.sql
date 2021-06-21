-- This hive script will be used in the oozie workflow.
DROP TABLE movies;

-- "create external table" just creates an abstraction of the existing table on disk, but doesn't make a copy of it.
-- We will pull data from MySQL using Sqoop. After sqoop has pulled data from MySQL, this hive script will create "movies" table based on that abstract.
CREATE EXTERNAL TABLE movies (movie_id INT, title STRING, release DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/maria_dev/movies/';

-- "OUTPUT" will be setup in the properties file of the oozie job.
-- We will extract any movies released before 1940.
INSERT OVERWRITE DIRECTORY '${OUTPUT}' SELECT * FROM movies WHERE release < '1940-01-01' ORDER BY release;
