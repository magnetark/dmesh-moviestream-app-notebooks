
-- TAGS COUNT
SELECT count(*) FROM tags;

-- TAGS SAMPLE
SELECT * FROM TAGS LIMIT 100;

-- GET MAX INDEX
SELECT max(index) as max_index
FROM tags
LIMIT 1;

