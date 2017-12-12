SELECT tag
FROM
(
    SELECT tag, COUNT(*) as cnt
    FROM tags
    GROUP BY tag
    SORT BY cnt DESC
) AS tmp
LIMIT 10;
