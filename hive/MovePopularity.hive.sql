-- Hive uses schema-on-read. 
-- Data are still stored as files without structure. Hive maintains a metastore that holds the schema.
-- Hive is only for OLAP, NOT OLTP

-- These are the code to load the file if we were to NOT use Ambari UI
-- Hive does NOT create a RDB under the hood.
/*
CREATE TABLE ratings (
    userID INT,
    movieID INT,
    rating INT,
    time INT
)
ROW FORMAT DELIMITED
FILES TERMINATED BY '\t'
STORED AS TEXTFILE

-- LOAD DATA will MOVE file (typically currently already on distributed file system) into Hive
-- LOAD DATA LOCAL will COPY file from local file system to Hive
-- In both cases Hive is taking ownership of the tables (called Managed tables.), so a DROP TABLE command will permanently delete the tables.
-- If this is not desired, use External Tables instead, that way DROP TABLE will only drop the metadata.
-- We can also partition the data on Hive but the partition is mainly for creating read hot-spots, not for distributing write traffic.

LOAD DATA LOCAL INPATH `${env:HOME}/ml-100k/u.data`
OVERWRITE INTO TABLE ratings;
*/


CREATE VIEW IF NOT EXISTS topMovieIDs AS 
SELECT movieID, count(movieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY ratingCount DESC;

SELECT n.title, ratingCount
FROM topMovieIDs t JOIN names n ON t.movieID = n.movieID;