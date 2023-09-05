SELECT *
FROM {{ source('events_raw_source', 'events') }}
WHERE country = 'USA'
GROUP BY id
ORDER BY id ASC
