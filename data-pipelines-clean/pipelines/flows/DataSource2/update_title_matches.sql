MERGE INTO {{ database }}.{{ schema }}.DATASOURCE2_TITLE_MATCH
USING (
    SELECT distinct
        TRIM(LOWER($1:"title"::text)) TITLE,
        $1:"title_DataSource2_id"::text TITLE_DATASOURCE2_ID,
        $1:"itemset_DataSource2_id"::text ITEMSET_DATASOURCE2_ID
    FROM @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }}(file_format => {{ database }}.{{ schema }}."PARQUET_FORMAT")
) temp on temp.TITLE = {{ database }}.{{ schema }}.DATASOURCE2_TITLE_MATCH.TITLE
WHEN MATCHED THEN
    UPDATE SET
        TITLE_DATASOURCE2_ID = temp.TITLE_DATASOURCE2_ID,
        ITEMSET_DATASOURCE2_ID = temp.ITEMSET_DATASOURCE2_ID,
        HAS_MATCH = TRUE,
        CLIENTNAME_MATCH_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (TITLE, TITLE_DATASOURCE2_ID, ITEMSET_DATASOURCE2_ID, HAS_MATCH, CLIENTNAME_MATCH_TS)
    VALUES (temp.TITLE, temp.TITLE_DATASOURCE2_ID, temp.ITEMSET_DATASOURCE2_ID, TRUE, CURRENT_TIMESTAMP())