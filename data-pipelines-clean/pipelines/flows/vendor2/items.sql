copy into {{ database }}.{{ schema }}.VENDOR2_DATA_ITEMS (
    "ITEM_ID",
    "SHOW_ID",
    "TITLE",
    "ITEM_IMAGE_URL",
    "OVERVIEW",
    "RUNTIME",
    "SEQUENCE_NUMBER",
    "ITEM_NUMBER",
    "RELEASED_ON",
    "DATASOURCE2_ID",
    "SOURCE_FILENAME",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"item_id"::text,
        $1:"show_id"::text,
        $1:"title"::text,
        $1:"item_image_url"::text,
        $1:"overview"::text,
        TRY_TO_NUMBER($1:"runtime"::text),
        $1:"sequence_number"::text,
        $1:"item_number"::text,
        TRY_TO_TIMESTAMP_NTZ($1:"released_on"::text),
        $1:"DataSource2"::text,
        $1:"source_filename"::text,
        metadata$filename,
        current_timestamp(),
        '{{ flow_run_id }}'
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE


