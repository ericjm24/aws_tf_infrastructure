copy into {{ database }}.{{ schema }}.VENDOR2_DATA_GLOBAL_ITEM_AVAILABILITY (
    "ITEM_ID",
    "REGION_ID",
    "SERVICE_AVAILABILITY",
    "SOURCE_FILENAME",
    "REPORT_TS",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"item_id"::text,
        $1:"region_id"::text,
        $1:"service_availability"::array,
        $1:"source_filename"::text,
        $1:"report_ts":timestamp_ntz,
        metadata$filename,
        current_timestamp(),
        '{{ flow_run_id }}'
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE


