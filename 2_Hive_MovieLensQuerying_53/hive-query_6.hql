SELECT m.title, tmp.cnt
FROM
(
    SELECT movieId, COUNT(*) as cnt
    FROM tags
    WHERE tag LIKE 'sci-fi'
    GROUP BY movieId
    SORT BY cnt DESC
    LIMIT 15
) AS tmp
JOIN movies m
    ON m.movieId = tmp.movieId;
