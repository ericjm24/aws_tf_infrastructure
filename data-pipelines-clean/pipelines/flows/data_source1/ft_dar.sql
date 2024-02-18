copy into {{ database }}.{{ schema }}.DATASOURCE1_DETAILED_ASSET_REPORT (
    "DATE",
    "NODE_ID",
    "NODE_TITLE",
    "TOTAL_WATCH_DURATION",
    "COMPLETION_PERCENT",
    "UID",
    "CHANNEL_NAME",
    "CHANNEL_ID",
    "VENDOR6_VIEWS",
    "VENDOR3_VIEWS",
    "OTHER_VIEWS",
    "TOTAL_VIEWS",
    "UNIQUE_AD_REQUESTS",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        TO_DATE($1:"Date"::text, 'YYYY-MM-DD') DATE,     
        $1:"Node_ID"::bigint NODE_ID, 
        $1:"Node_Title"::text NODE_TITLE,     
        $1:"Total_Watch_Duration"::bigint TOTAL_WATCH_DURATION,     
        $1:"Completion %"::float COMPLETION_PERCENT,     
        $1:"UID"::bigint UID,     
        $1:"Channel_Name"::text CHANNEL_NAME,  
        $1:"Channel_ID"::bigint CHANNEL_ID,    
        $1:"Vendor6_Views"::int VENDOR6_VIEWS,    
        $1:" Vendor3_Views"::int VENDOR3_VIEWS,    
        $1:"Other_Views"::int OTHER_VIEWS,    
        $1:"Total_Views"::int TOTAL_VIEWS,    
        $1:"Unique_Ad_Requests"::int UNIQUE_AD_REQUESTS,
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

