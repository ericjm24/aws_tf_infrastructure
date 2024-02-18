copy into {{ database }}.{{ schema }}.VENDOR2_DATA_SHOWS (
    "SHOW_ID",
    "TITLE",
    "POSTER_URL",
    "BACKDROP_URL",
    "OVERVIEW",
    "SHOW_CAST",
    "POPULARITY",
    "CLASSIFICATION",
    "RUNTIME",
    "GENRES",
    "TAGS",
    "RELEASED_ON",
    "VENDOR2_URL",
    "STATUS",
    "PRODUCTION_COMPANY",
    "TRAILER_URL",
    "NETWORK",
    "LANGUAGE",
    "DATASOURCE2_ID",
    "SOURCE_FILENAME",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"show_id"::text,
        $1:"title"::text,
        $1:"poster_url"::text,
        $1:"backdrop_url"::text,
        $1:"overview"::text,
        $1:"show_cast"::ARRAY,
        TRY_TO_NUMBER($1:"popularity"::text, 10, 5),
        $1:"classification"::text,
        TRY_TO_NUMBER($1:"runtime"::text),
        $1:"genres"::ARRAY,
        $1:"tags"::ARRAY,
        TRY_TO_DATE($1:"released_on"::text, 'YYYY-MM-DD'),
        $1:"vendor2_url"::text,
        $1:"status"::text,
        $1:"production_company"::ARRAY,
        $1:"trailer_url"::ARRAY,
        $1:"network"::ARRAY,
        $1:"language"::text,
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


