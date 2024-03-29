copy into {{ database }}.{{ schema }}.vendor1_finance (
    "AD_IMPRESSIONS",
    "ANNOTATION_CLICKTHROUGH_RATE",
    "ANNOTATION_CLICKABLE_IMPRESSIONS",
    "ANNOTATION_CLICKS",
    "ANNOTATION_CLOSABLE_IMPRESSIONS",
    "ANNOTATION_CLOSE_RATE",
    "ANNOTATION_CLOSES",
    "ANNOTATION_IMPRESSIONS",
    "AVERAGE_VIEW_DURATION",
    "AVERAGE_VIEW_PCT",
    "CARD_CLICK_RATE",
    "CARD_CLICKS",
    "CARD_IMPRESSIONS",
    "CARD_TEASER_CLICKRATE",
    "CARD_TEASER_CLICKS",
    "CARD_TEASER_IMPRESSIONS",
    "CHANNEL_ID",
    "CHANNEL_NAME",
    "COMMENT_DESC",
    "COUNTRY_CD",
    "CPM",
    "DISLIKES_NBR",
    "END_DATE",
    "EST_AD_REVENUE",
    "EST_MINUTES_WATCHED",
    "EST_REDMINUTES_WATCHED",
    "EST_REDPARTNER_REVENUE",
    "EST_REVENUE",
    "GROSS_REVENUE",
    "LIKES_NBR",
    "MONETIZED_PLAYBACKS",
    "PLAYBACK_BASED_CPM",
    "REDVIEWS_NBR",
    "SHARES_NBR",
    "START_DATE",
    "SUBSCRIBERS_GAINED_NBR",
    "SUBSCRIBERS_LOST",
    "VENDOR1_ITEM_ID",
    "ITEMS_ADDED_TO_PLAYLISTS",
    "ITEMS_REMOVED_FROM_PLAYLISTS",
    "VIEWS_NBR",
    "CLIENTNAME_SOURCE_KEY",
    "CLIENTNAME_LOADED_TS",
    "CLIENTNAME_START_DATE",
    "CLIENTNAME_END_DATE",
    "CLIENTNAME_FLOW_RUN_ID"
) from ( 
    select
        $1:adImpressions::text AD_IMPRESSIONS,
        $1:annotationClickThroughRate::number ANNOTATION_CLICKTHROUGH_RATE,
        $1:annotationClickableImpressions::number ANNOTATION_CLICKABLE_IMPRESSIONS,
        $1:annotationClicks::number ANNOTATION_CLICKS,
        $1:annotationClosableImpressions::number ANNOTATION_CLOSABLE_IMPRESSIONS,
        $1:annotationCloseRate::number ANNOTATION_CLOSE_RATE,
        $1:annotationCloses::number ANNOTATION_CLOSES,
        $1:annotationImpressions::number ANNOTATION_IMPRESSIONS,
        $1:averageViewDuration::number AVERAGE_VIEW_DURATION,
        $1:averageViewPercentage::double AVERAGE_VIEW_PCT,
        $1:cardClickRate::number CARD_CLICK_RATE,
        $1:cardClicks::number CARD_CLICKS,
        $1:cardImpressions::number CARD_IMPRESSIONS,
        $1:cardTeasertClickRate::number CARD_TEASER_CLICKRATE,
        $1:cardTeaserClicks::number CARD_TEASER_CLICKS,
        $1:cardTeaserImpressions::number CARD_TEASER_IMPRESSIONS,
        $1:channel_id::text CHANNEL_ID,
        '{{ channel_name }}' CHANNEL_NAME,
        $1:comments::number COMMENT_DESC,
        $1:country::text COUNTRY_CD,
        $1:cpm::double CPM,
        $1:dislikes::number DISLIKES_NBR,
        $1:end_date::text END_DATE,
        $1:estimatedAdRevenue::double EST_AD_REVENUE,
        $1:estimatedMinutesWatched::number EST_MINUTES_WATCHED,
        $1:estimatedRedMinutesWatched::number EST_REDMINUTES_WATCHED,
        $1:estimatedRedPartnerRevenue::double EST_REDPARTNER_REVENUE,
        $1:estimatedRevenue::double EST_REVENUE,
        $1:grossRevenue::double GROSS_REVENUE,
        $1:likes::number LIKES_NBR,
        $1:monetizedPlaybacks::number MONETIZED_PLAYBACKS,
        $1:playbackBasedCpm::double PLAYBACK_BASED_CPM,
        $1:redViews::number REDVIEWS_NBR,
        $1:shares::number SHARES_NBR,
        $1:start_date::text START_DATE,
        $1:subscribersGained::number SUBSCRIBERS_GAINED_NBR,
        $1:subscribersLost::number SUBSCRIBERS_LOST,
        $1:item_id::text VENDOR1_ITEM_ID,
        $1:itemsAddedToPlaylists::number ITEMS_ADDED_TO_PLAYLISTS,
        $1:itemsRemovedFromPlaylists::number ITEMS_REMOVED_FROM_PLAYLISTS,
        $1:views::number VIEWS_NBR,
        metadata$filename CLIENTNAME_SOURCE_KEY,
        current_timestamp() as CLIENTNAME_LOADED_TS,
        $1:start_date::DATE CLIENTNAME_START_DATE,
        $1:end_date::DATE CLIENTNAME_END_DATE,
        '{{ flow_run_id }}'
    from
        @{{ database }}.{{ schema }}.{{ mount_location }}/{{ file_name }})
    file_format = (
        TYPE = 'PARQUET' 
        COMPRESSION = 'AUTO')
    FORCE = TRUE


