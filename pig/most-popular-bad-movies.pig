ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealese:chararray, imdblink:chararray);
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

ratingsByMovie = GROUP ratings BY movieID;
avgRatingsAndPopularity = FOREACH ratingsByMovie GENERATE group as movieID, AVG(ratings.rating) as avgRating, COUNT(ratings.rating) as popularity;

oneStarMovies = FILTER avgRatingsAndPopularity BY avgRating < 2.0;
oneStarsWithData = JOIN oneStarMovies BY movieID, nameLookup BY movieID;

-- Only select the useful columns from oneStarsWithData
oneStarsWithSelectedColumns = FOREACH oneStarsWithData GENERATE nameLookup::movieTitle as movieName,
	oneStarMovies::avgRating AS avgRating, oneStarMovies::popularity AS popularity;

mostPopularOneStarMovies = ORDER oneStarsWithSelectedColumns BY popularity DESC;
DUMP mostPopularOneStarMovies;