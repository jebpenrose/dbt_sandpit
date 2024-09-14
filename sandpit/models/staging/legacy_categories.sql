
{{config(materialized='table')}}
-- Could be in incremental to avoid reprocessing.
-- But - if we ever did add records - would have reprocess to see if there are any new dupes anyway.
WITH base AS (
    SELECT narrative, category, COUNT(*) AS times_used, MAX(recorded_date) AS last_use
    FROM {{ref('stg_spending')}}
    --WHERE recorded_date >= '2023-01-01' AND narrative LIKE 'AMAZON MARKETPLACE AU SYDNEY SOUT AUS'
    GROUP BY 1, 2
    ORDER BY COUNT(*) DESC

), de_dupe AS (
    SELECT narrative, category, times_used, last_use
        ,COUNT(narrative) OVER(PARTITION BY narrative) AS num_dupes
        --,ROW_NUMBER() OVER(PARTITION BY narrative ORDER BY last_use DESC) AS precedence
        --,RANK() OVER(PARTITION BY narrative ORDER BY last_use DESC) AS precedence
    FROM base
)
SELECT narrative, category, times_used, last_use
FROM de_dupe
WHERE num_dupes=1
--ORDER BY num_dupes DESC, narrative, precedence