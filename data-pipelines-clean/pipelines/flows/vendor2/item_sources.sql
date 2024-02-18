copy into {{ database }}.{{ schema }}.VENDOR2_DATA_ITEM_SOURCES (
    "ITEM_ID",
    "PURCHASE_COST_HD",
    "PURCHASE_COST_SD",
    "REGION_ID",
    "RENTAL_COST_SD",
    "RENTAL_COST_HD",
    "SOURCE_ID",
    "SOURCE_FILENAME",
    "REPORT_TS",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"item_id"::text,
        $1:"purchase_cost_hd"::text,
        $1:"purchase_cost_sd"::text,
        $1:"region_id"::text,
        $1:"rental_cost_hd"::text,
        $1:"rental_cost_sd"::text,
        $1:"source_id"::text,
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


