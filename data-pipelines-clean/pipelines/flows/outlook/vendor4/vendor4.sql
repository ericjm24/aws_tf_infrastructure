copy into {{ database }}.{{ schema }}.VENDOR4_REVENUE (
    "UPC",
    "ISRC_OR_ISBN",
    "VENDOR_IDENTIFIER",
    "QUANTITY",
    "PARTNER_SHARE",
    "EXTENDED_PARTNER_SHARE",
    "PARTNER_SHARE_CURRENCY",
    "SALES_OR_RETURN",
    "APPLE_IDENTIFIER",
    "SHOW_OR_DEVELOPER",
    "TITLE",
    "STUDIO_OR_NETWORK",
    "GRID",
    "PRODUCT_TYPE_IDENTIFIER",
    "ISAN_OR_OTHER_IDENTIFIER",
    "COUNTRY_OF_SALE",
    "PREORDER_FLAG",
    "PROMO_CODE",
    "CUSTOMER_PRICE",
    "CUSTOMER_CURRENCY",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:"UPC"::text,
        $1:"ISRC/ISBNP"::text,
        $1:"Vendor Identifier"::text,
        $1:"Quantity"::int,
        $1:"Partner Share"::float,
        $1:"Extended Partner Share"::float,
        $1:"Parner Share Currency"::text,
        $1:"Sales or Return"::text,
        $1:"Apple Identifier"::bigint,
        $1:"Artist/Show/Developer/Author"::text,
        $1:"Title"::text,
        $1:"Label/Studio/Network/Developer/Publisher"::text,
        $1:"Grid"::text,
        $1:"Product Type Identifier"::text,
        $1:"ISAN/Other Identifier"::text,
        $1:"Country of Sale"::text,
        $1:"Pre-order Flag"::text,
        $1:"Promo Code"::text,
        $1:"Customer Price"::float,
        $1:"Customer Currency"::text,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        DATE(TO_TIMESTAMP($1:"Start Date"::text)) CLIENTNAME_START_DATE,
        DATE(TO_TIMESTAMP($1:"End Date"::text)) CLIENTNAME_END_DATE,
        '{{ flow_run_id }}' as CLIENTNAME_FLOW_RUN_ID
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE
