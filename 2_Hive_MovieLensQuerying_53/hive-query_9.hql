SELECT *
FROM genome_scores s
JOIN genome_tags t
    ON t.tagId = s.tagId
WHERE s.movieId = 18
SORT BY s.relavance DESC
LIMIT 15;
