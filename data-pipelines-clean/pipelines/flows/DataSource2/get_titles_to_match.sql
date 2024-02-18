SELECT distinct temp.TITLE
FROM (
    select TRIM(LOWER(TITLE)) as TITLE
    from {{ database }}.{{ metrics_schema }}.MAP_TITLE_MATCH
    WHERE DATASOURCE2_ID is NULL
        and ITEMSET_DATASOURCE2_ID is null
        AND TITLE IS NOT NULL
        and LENGTH(TRIM(TITLE)) > 0
) temp
WHERE not temp.TITLE in (
    select TITLE from {{ database }}.{{ stage_schema }}.DATASOURCE2_TITLE_MATCH
    where HAS_MATCH = TRUE
)
LIMIT {{ limit }}