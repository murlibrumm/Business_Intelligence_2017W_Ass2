SELECT m.title, s.relevance
FROM genome_scores s
JOIN (SELECT * FROM genome_tags WHERE tag LIKE 'vienna') AS tmp
    ON s.tagId = tmp.tagId
JOIN movies m
    ON s.movieId = m.movieId
SORT BY s.relevance DESC
LIMIT 10;
