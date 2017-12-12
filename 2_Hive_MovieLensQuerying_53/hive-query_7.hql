SELECT m.title
FROM
(
    SELECT movieId, AVG(rating) AS avg_rating
    FROM ratings
    GROUP BY movieId
    HAVING COUNT(*) > 500
    SORT BY avg_rating DESC
    LIMIT 10
) AS tmp
JOIN movies m
    ON m.movieId = tmp.movieId;
