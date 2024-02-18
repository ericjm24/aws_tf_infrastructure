copy into {{ database }}.{{ schema }}.VENDOR1_DEVICE_TYPE (
    "DATE",
    "CHANNEL_ID",
    "CHANNEL_NAME",
    "ITEM_ID",
    "LIVE_ON_DEMAND",
    "SUBSCRIBED_STATUS",
    "COUNTRY_CODE",
    "DEVICE_TYPE",
    "OPERATING_SYSTEM",
    "VIEWS",
    "WATCH_TIME_MINUTES",
    "AVERAGE_VIEW_DURATION_SECONDS",
    "RED_VIEWS",
    "RED_WATCH_TIME_MINUTES",
    "AVERAGE_VIEW_DURATION_PERCENTAGE",
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
        $1:device_type::text DEVICE_TYPE,
        $1:operating_system::text OPERATING_SYSTEM,
        $1:views::number VIEWS,
        $1:watch_time_minutes::number WATCH_TIME_MINUTES,
        $1:average_view_duration_seconds::double AVERAGE_VIEW_DURATION_SECONDS,
        $1:average_view_duration_percentage::double AVERAGE_VIEW_DURATION_PERCENTAGE,
        $1:red_views::number RED_VIEWS,
        $1:red_watch_time_minutes::number RED_WATCH_TIME_MINUTES,
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


