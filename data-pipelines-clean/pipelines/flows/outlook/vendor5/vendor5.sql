copy into {{ database }}.{{ schema }}.VENDOR5_MONETIZATION (
    "CONTENT_PARTNER",
    "ITEMSET_NAME",
    "ITEM_NAME",
    "CLIP_NAME",
    "TVMS",
    "SESSIONS",
    "REVENUE_PER_CLIP",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"Content Partner"::text,
        $1:"ItemSet Name"::text,
        $1:"Item Name"::text,
        $1:"Clip Name"::text,
        $1:"TVMs"::int,
        $1:"Sessions"::int,
        $1:"Revenue per Clip"::float,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        TO_DATE('{{ start_date }}', 'YYYY-MM-DD') CLIENTNAME_START_DATE,
        TO_DATE('{{ end_date }}', 'YYYY-MM-DD') CLIENTNAME_END_DATE,
        '{{ flow_run_id }}' as CLIENTNAME_FLOW_RUN_ID
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE
