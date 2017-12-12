SELECT *
FROM genome_scores s
JOIN movies m
    ON s.movieId = m.movieId
JOIN genome_tags t
    ON s.tagId = t.tagId
WHERE t.tag LIKE 'vienna'
SORT BY s.relavance
LIMIT 10;
