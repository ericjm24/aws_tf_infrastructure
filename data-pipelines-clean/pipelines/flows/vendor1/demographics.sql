copy into {{ database }}.{{ schema }}.VENDOR1_DEMOGRAPHICS (
    "DATE",
    "CHANNEL_ID",
    "CHANNEL_NAME",
    "ITEM_ID",
    "LIVE_OR_ON_DEMAND",
    "SUBSCRIBED_STATUS",
    "COUNTRY_CODE",
    "AGE_GROUP",
    "GENDER",
    "VIEWS_PERCENTAGE",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:date::text DATE,
        $1:channel_id::text CHANNEL_ID,
        '{{ channel_name }}' CHANNEL_NAME,
        $1:item_id::text VENDOR1_ITEM_ID,
        $1:live_or_on_demand::text LIVE_OR_ON_DEMAND,
        $1:subscribed_status::text SUBSCRIBED_STATUS,
        $1:country_code::text COUNTRY_CODE,
        $1:age_group::text AGE_GROUP,
        $1:gender::text GENDER,
        $1:views_percentage::float VIEWS_PERCENTAGE,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        TO_DATE($1:date::text, 'YYYYMMDD') CLIENTNAME_START_DATE,
        DATEADD(day, 1, TO_DATE($1:date::text, 'YYYYMMDD')) CLIENTNAME_END_DATE,
        '{{ flow_run_id }}'
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE


