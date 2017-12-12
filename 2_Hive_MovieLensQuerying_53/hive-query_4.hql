SELECT m.title, VARIANCE(r.rating) AS var
FROM movies m
JOIN ratings r
    ON m.movieId = r.movieId
WHERE cast(r.`timestamp` as int) >= 1262304000
    AND cast(r.`timestamp` as int) < 1293840000
GROUP BY m.title
SORT BY var DESC
LIMIT 10;
