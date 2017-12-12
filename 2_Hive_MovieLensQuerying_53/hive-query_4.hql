SELECT m.title, VARIANCE(r.rating) AS var
FROM movies m
JOIN ratings r
    ON m.movieId = r.movieId
WHERE r.`timestamp` >= '2010-01-01'
    AND r.`timestamp` <= '2010-12-31'
GROUP BY m.title
SORT BY var DESC
LIMIT 10;
