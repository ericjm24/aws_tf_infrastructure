copy into {{ database }}.{{ schema }}.VENDOR3_REVENUE (
    "TITLE_OR_ITEMSET_ID",
    "ASIN",
    "TITLE_NAME",
    "ITEMSET_NAME",
    "ITEMSET_NAME",
    "CONSUMPTION_TYPE",
    "QUALITY",
    "UNIT_PRICE",
    "QUANTITY",
    "IS_REFUND",
    "DURATION_STREAMED",
    "PAID_AD_IMPRESSIONS",
    "AD_REVENUE",
    "ROYALTY_RATE",
    "ROYALTY_AMOUNT",
    "ROYALTY_CURRENCY",
    "REGION",
    "TERRITORY",
    "TYPE",
    "CER_PERCENTILE",
    "REPORT_TYPE",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"Title/ItemSet ID"::text,     
        $1:"ASIN"::text, 
        $1:"Title Name"::text,     
        $1:"ItemSet Name"::text,     
        $1:"ItemSet Name"::text,     
        $1:"Consumption Type"::text,     
        $1:"Quality"::text,     
        CAST(REPLACE($1:"Unit Price"::text, ',', '') as FLOAT),    
        CAST(REPLACE($1:"Quantity"::text, ',', '') as FLOAT),    
        $1:"Is Refund"::text,     
        CAST(REPLACE($1:"Duration Streamed"::text, ',', '') as FLOAT),    
        CAST(REPLACE($1:"Paid Ad Impressions"::text, ',', '') as FLOAT),     
        CAST(REPLACE($1:"Ad Revenue"::text, ',', '') as FLOAT),   
        CAST(REPLACE($1:"Royalty Rate"::text, ',', '') as FLOAT),   
        CAST(REPLACE($1:"Royalty Amount"::text, ',', '') as FLOAT),    
        $1:"Royalty Currency"::text,    
        $1:"Region"::text,    
        $1:"Territory"::text,
        $1:"Type"::text,    
        CAST(REPLACE($1:"CER Percentile"::text, '%', '') as FLOAT)*0.01,
        '{{ report_type }}' REPORT_TYPE,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        TO_DATE($1:"Period Start"::text, 'YYYY-MM-DD') CLIENTNAME_START_DATE,
        TO_DATE($1:"Period End"::text, 'YYYY-MM-DD') CLIENTNAME_END_DATE,
        '{{ flow_run_id }}' FLOW_RUN_ID
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE

