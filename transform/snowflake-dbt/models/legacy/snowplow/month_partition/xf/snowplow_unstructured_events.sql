with
    events as (select * from {{ ref("snowplow_unnested_events") }}),
    renamed as (

        select

            event_id::varchar as event_id,
            event_name::varchar as event_name,
            iff(
                unstruct_event = 'masked', 'masked', try_parse_json(unstruct_event)
            )::variant as unstruct_event,
            iff(
                unstruct_event = 'masked',
                'masked',
                try_parse_json(unstruct_event)['data']['data']
            )::variant as unstruct_event_data,
            v_tracker::varchar as v_tracker,
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

            -- standard context
            gsc_environment as gsc_environment,
            gsc_extra as gsc_extra,
            gsc_namespace_id as gsc_namespace_id,
            gsc_plan as gsc_plan,
            gsc_google_analytics_client_id as gsc_google_analytics_client_id,
            gsc_project_id as gsc_project_id,
            gsc_pseudonymized_user_id as gsc_pseudonymized_user_id,

            -- change_form
            cf_formid::varchar as cf_formid,
            cf_elementid::varchar as cf_elementid,
            cf_nodename::varchar as cf_nodename,
            cf_type::varchar as cf_type,
            cf_elementclasses::varchar as cf_elementclasses,
            -- submit_form
            sf_formid::varchar as sf_formid,
            sf_formclasses::varchar as sf_formclasses,
            -- focus_form
            ff_formid::varchar as ff_formid,
            ff_elementid::varchar as ff_elementid,
            ff_nodename::varchar as ff_nodename,
            ff_elementtype::varchar as ff_elementtype,
            ff_elementclasses::varchar as ff_elementclasses,
            -- link_click
            lc_elementcontent::varchar as lc_elementcontent,
            lc_elementid::varchar as lc_elementid,
            lc_elementclasses::varchar as lc_elementclasses,
            lc_elementtarget::varchar as lc_elementtarget,
            lc_targeturl::varchar as lc_targeturl

        from events
        where event = 'unstruct'

    )

select *
from renamed
