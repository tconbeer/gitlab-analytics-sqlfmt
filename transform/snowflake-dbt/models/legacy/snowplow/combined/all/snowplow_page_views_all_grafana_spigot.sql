{{ config({"materialized": "view"}) }}

with snowplow_page_views_all as (select * from {{ ref("snowplow_page_views_all") }})

select
    min_tstamp,
    session_index,
    page_view_id,
    page_view_index,
    page_view_in_session_index,
    page_view_start,
    page_view_end,
    page_view_start_local,
    page_view_end_local,
    horizontal_pixels_scrolled,
    vertical_pixels_scrolled,
    horizontal_percentage_scrolled,
    vertical_percentage_scrolled,
    vertical_percentage_scrolled_tier,
    page_url,
    page_url_scheme,
    page_url_host,
    page_url_port,
    page_url_path,
    page_url_query,
    page_url_fragment,
    page_title,
    page_width,
    page_height,
    referer_url,
    referer_url_scheme,
    referer_url_host,
    referer_url_port,
    referer_url_path,
    referer_url_query,
    referer_url_fragment,
    referer_medium,
    referer_source,
    referer_term,
    app_id,
    browser,
    browser_name,
    browser_major_version,
    browser_minor_version,
    browser_build_version,
    os,
    os_name,
    os_major_version,
    os_minor_version,
    os_build_version,
    device,
    browser_window_width,
    browser_window_height,
    browser_language,
    os_manufacturer,
    os_timezone,
    redirect_time_in_ms,
    unload_time_in_ms,
    app_cache_time_in_ms,
    dns_time_in_ms,
    tcp_time_in_ms,
    request_time_in_ms,
    response_time_in_ms,
    processing_time_in_ms,
    dom_loading_to_interactive_time_in_ms,
    dom_interactive_to_complete_time_in_ms,
    onload_time_in_ms,
    total_time_in_ms,
    browser_engine,
    device_type,
    device_is_mobile
from snowplow_page_views_all
