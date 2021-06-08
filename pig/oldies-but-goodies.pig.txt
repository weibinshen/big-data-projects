-- Filter out movies that are rated above 4 on average, and sort by release date in ascending order.
-- This script is written for the MovieLens 100k dataset. 

-- Load in the ratings and metadata.
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealese:chararray, imdblink:chararray);

-- Convert the date into timestamps so we can sort on it later.
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

-- Calculate the average ratings for each move, by first grouping(bagging) them, and then using the AVG() function
-- 'group' is the default name PIG gives to the grouping key.
ratingsByMovie = GROUP ratings BY movieID;
avgRatings = FOREACH ratingsByMovie GENERATE group as movieID, AVG(ratings.rating) as avgRating;

-- Filter out 4+ rated movies
fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

-- Join the filtered moves with the nameLookup which contains movie names and release date as timestamps.
fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

-- Sort by release time by ascending order.
oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

-- Write the result to console. Use STORE command to save to a file.
DUMP oldestFiveStarMovies;

-- To inspect the schema of a relation (in PIG the data frames are called relations), use 'DESCRIBE <relation name>;'
-- Pig also have EXPLAIN/ILLUSTRATE similar to sql explain.
-- User defined functions (UDFs) can be defined using Java and by compiling jar files. Keywords relating to importing jars: REGISTER, DEFINE, IMPORT.