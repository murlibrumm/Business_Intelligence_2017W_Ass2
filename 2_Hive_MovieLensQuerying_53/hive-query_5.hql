SELECT m.title, tmp.cnt
FROM
(
    SELECT movieId, COUNT(*) as cnt
    FROM tags
    GROUP BY movieId
    SORT BY cnt DESC
    LIMIT 10
) AS tmp
JOIN movies m
    ON m.movieId = tmp.movieId;
