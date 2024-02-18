copy into {{ database }}.{{ schema }}.VENDOR2_DATA_SOURCES (
    "ACCESS_TYPE",
    "DISPLAY_NAME",
    "ID",
    "SOURCE_NAME",
    "SOURCE_FILENAME",
    "REPORT_TS",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        TRY_TO_NUMBER($1:"access_type"::text),
        $1:"display_name"::text,
        $1:"id"::text,
        $1:"source_name"::text,
        $1:"source_filename"::text,
        TO_TIMESTAMP($1:"report_ts"::text),
        metadata$filename,
        current_timestamp(),
        '{{ flow_run_id }}'
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE


