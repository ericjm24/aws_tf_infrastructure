copy into {{ database }}.{{ schema }}.VENDOR7_REVENUE (
    "PROGRAMMER",
    "CHANNEL",
    "ASSET_TITLE",
    "ASSET_PROGRAM_NAME",
    "TOTAL_UNIQUE_USERS",
    "TOTAL_ITEM_START",
    "TOTAL_ITEM_COMPLETE",
    "SHARE_OF_MONTHLY_TOTAL",
    "IMPLIED_REVENUE",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"Programmer"::text,
        $1:"Channel"::text,
        $1:"asset_title"::text,
        $1:"asset_program_name"::text,
        $1:"Total Unique Users"::int,
        $1:"Total Item Start"::int,
        $1:"Total Item Complete"::int,
        $1:"Share of Monethly Total"::float,
        $1:"Implied Revenue"::float,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        DATE(TO_TIMESTAMP($1:"Report Month"::text)) CLIENTNAME_START_DATE,
        DATEADD(MONTH,1,DATE(TO_TIMESTAMP($1:"Report Month"::text))) CLIENTNAME_END_DATE,
        '{{ flow_run_id }}' as CLIENTNAME_FLOW_RUN_ID
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE
