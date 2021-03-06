SELECT title
FROM
(
    SELECT r.movieId as movieId, m.title AS title, AVG(r.rating) AS avg_rating
    FROM ratings r
    JOIN (SELECT * FROM movies WHERE genres LIKE '%Drama%') AS m
        ON m.movieId = r.movieId
    GROUP BY r.movieId, m.title
    HAVING COUNT(*) > 10
    SORT BY avg_rating DESC
) as tmp
LIMIT 10;
