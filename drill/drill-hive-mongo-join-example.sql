-- With Drill, we can write SQL to join two data sets from two non-relational sources.

SELECT u.occupation, COUNT(*) AS ratingCountPerOccupation
FROM hive.movielens.ratings r 
JOIN mongo.movielens.users u 
ON r.user_id = u.user_id 
GROUP BY u.occupation
ORDER BY ratingCountPerOccupation DESC