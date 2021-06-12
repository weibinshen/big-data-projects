CREATE VIEW IF NOT EXISTS avgRatings AS 
SELECT movieID, AVG(rating) as avgRating, COUNT(movieID) as popularity
FROM ratings
GROUP BY movieID
ORDER BY avgRating DESC;

SELECT n.title, avgRating, popularity
FROM avgRatings t JOIN names n ON t.movieID = n.movieID
WHERE popularity > 10;