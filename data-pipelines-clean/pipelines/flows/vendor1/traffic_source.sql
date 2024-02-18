copy into {{ database }}.{{ schema }}.VENDOR1_TRAFFIC_SOURCE (
    "AVERAGE_VIEW_DURATION_PERCENTAGE",
    "AVERAGE_VIEW_DURATION_SECONDS",
    "CHANNEL_ID",
    "CHANNEL_NAME",
    "COUNTRY_CODE",
    "DATE",
    "LIVE_OR_ON_DEMAND",
    "RED_VIEWS",
    "RED_WATCH_TIME_MINUTES",
    "SUBSCRIBED_STATUS",
    "TRAFFIC_SOURCE_DETAIL",
    "TRAFFIC_SOURCE_TYPE",
    "ITEM_ID",
    "VIEWS",
    "WATCH_TIME_MINUTES",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN"
) from ( 
    select
        $1:average_view_duration_percentage::double AVERAGE_VIEW_DURATION_PERCENTAGE,
        $1:average_view_duration_seconds::double AVERAGE_VIEW_DURATION_SECONDS,
        $1:channel_id::text CHANNEL_ID,
        '{{ channel_name }}' CHANNEL_NAME,
        $1:country_code::text COUNTRY_CODE,
        $1:date::text DATE,
        $1:live_or_on_demand::text LIVE_OR_ON_DEMAND,
        $1:red_views::number RED_VIEWS,
        $1:red_watch_time_minutes::number RED_WATCH_TIME_MINUTES,
        $1:subscribed_status::text SUBSCRIBED_STATUS,
        $1:traffic_source_detail::text TRAFFIC_SOURCE_DETAIL,
        $1:traffic_source_type::text TRAFFIC_SOURCE_TYPE,
        $1:item_id::text VENDOR1_ITEM_ID,
        $1:views::number VIEWS,
        $1:watch_time_minutes::number WATCH_TIME_MINUTES,
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


