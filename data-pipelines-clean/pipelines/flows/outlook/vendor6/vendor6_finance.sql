copy into {{ database }}.{{ schema }}.{{ table }} (
        "PAYOUT",
        "PROVIDER",
        "COUNTRY",
        "TITLE_NAME",
        "CLIENTNAME_SOURCE_KEY",
        "CLIENTNAME_LOADED_TS",
        "CLIENTNAME_START_DATE",
        "CLIENTNAME_END_DATE",
        "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:Payout::double,
        $1:ProviderName::text,
        $1:Territory::text,
        $1:Title::text,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        TO_DATE('{{ extra_cols.start_date }}', 'YYYY-MM-DD') CLIENTNAME_START_DATE,
        TO_DATE('{{ extra_cols.end_date }}', 'YYYY-MM-DD') CLIENTNAME_END_DATE,
        '{{ flow_run_id }}' as CLIENTNAME_FLOW_RUN_ID
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE
