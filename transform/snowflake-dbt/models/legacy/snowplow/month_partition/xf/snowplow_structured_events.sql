with
    events as (select * from {{ ref("snowplow_unnested_events") }}),
    renamed as (

        select

            event_id::varchar as event_id,
            v_tracker::varchar as v_tracker,
            se_action::varchar as event_action,
            se_category::varchar as event_category,
            se_label::varchar as event_label,
            se_property::varchar as event_property,
            se_value::varchar as event_value,
            try_parse_json(contexts)::variant as contexts,
            dvce_created_tstamp::timestamp as dvce_created_tstamp,
            derived_tstamp::timestamp as derived_tstamp,
            collector_tstamp::timestamp as collector_tstamp,
            user_id::varchar as user_custom_id,
            domain_userid::varchar as user_snowplow_domain_id,
            network_userid::varchar as user_snowplow_crossdomain_id,
            domain_sessionid::varchar as session_id,
            domain_sessionidx::int as session_index,
            (page_urlhost || page_urlpath)::varchar as page_url,
            page_urlscheme::varchar as page_url_scheme,
            page_urlhost::varchar as page_url_host,
            page_urlpath::varchar as page_url_path,
            page_urlfragment::varchar as page_url_fragment,
            mkt_medium::varchar as marketing_medium,
            mkt_source::varchar as marketing_source,
            mkt_term::varchar as marketing_term,
            mkt_content::varchar as marketing_content,
            mkt_campaign::varchar as marketing_campaign,
            app_id::varchar as app_id,
            br_family::varchar as browser_name,
            br_name::varchar as browser_major_version,
            br_version::varchar as browser_minor_version,
            os_family::varchar as os,
            os_name::varchar as os_name,
            br_lang::varchar as browser_language,
            os_manufacturer::varchar as os_manufacturer,
            os_timezone::varchar as os_timezone,
            br_renderengine::varchar as browser_engine,
            dvce_type::varchar as device_type,
            dvce_ismobile::boolean as device_is_mobile,
            gsc_environment as gsc_environment,
            gsc_extra as gsc_extra,
            gsc_namespace_id as gsc_namespace_id,
            gsc_plan as gsc_plan,
            gsc_google_analytics_client_id as gsc_google_analytics_client_id,
            gsc_project_id as gsc_project_id,
            gsc_pseudonymized_user_id as gsc_pseudonymized_user_id,
            gsc_source as gsc_source

        from events
        where event = 'struct'

    )

select *
from renamed
