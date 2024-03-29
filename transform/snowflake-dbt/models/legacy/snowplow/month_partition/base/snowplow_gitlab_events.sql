{% set year_value = var("year", run_started_at.strftime("%Y")) %}
{% set month_value = var("month", run_started_at.strftime("%m")) %}

{{ config({"unique_key": "event_id"}) }}

{% set change_form = [
    "formId",
    "elementId",
    "nodeName",
    "type",
    "elementClasses",
    "value",
] %}
{% set submit_form = ["formId", "formClasses", "elements"] %}
{% set focus_form = [
    "formId",
    "elementId",
    "nodeName",
    "elementType",
    "elementClasses",
    "value",
] %}
{% set link_click = [
    "elementId",
    "elementClasses",
    "elementTarget",
    "targetUrl",
    "elementContent",
] %}
{% set track_timing = ["category", "variable", "timing", "label"] %}


with
    filtered_source as (

        select
            app_id,
            base_currency,
            br_colordepth,
            br_cookies,
            br_family,
            br_features_director,
            br_features_flash,
            br_features_gears,
            br_features_java,
            br_features_pdf,
            br_features_quicktime,
            br_features_realplayer,
            br_features_silverlight,
            br_features_windowsmedia,
            br_lang,
            br_name,
            br_renderengine,
            br_type,
            br_version,
            br_viewheight,
            br_viewwidth,
            collector_tstamp,
            contexts,
            derived_contexts,
            -- correcting bugs on ruby tracker which was sending wrong timestamp
            -- https://gitlab.com/gitlab-data/analytics/issues/3097
            iff(
                date_part('year', try_to_timestamp(derived_tstamp)) > 1970,
                derived_tstamp,
                collector_tstamp
            ) as derived_tstamp,
            doc_charset,
            try_to_numeric(doc_height) as doc_height,
            try_to_numeric(doc_width) as doc_width,
            domain_sessionid,
            domain_sessionidx,
            domain_userid,
            dvce_created_tstamp,
            dvce_ismobile,
            dvce_screenheight,
            dvce_screenwidth,
            dvce_sent_tstamp,
            dvce_type,
            etl_tags,
            etl_tstamp,
            event,
            event_fingerprint,
            event_format,
            event_id,
            event_name,
            event_vendor,
            event_version,
            geo_city,
            geo_country,
            geo_latitude,
            geo_longitude,
            geo_region,
            geo_region_name,
            geo_timezone,
            geo_zipcode,
            ip_domain,
            ip_isp,
            ip_netspeed,
            ip_organization,
            mkt_campaign,
            mkt_clickid,
            mkt_content,
            mkt_medium,
            mkt_network,
            mkt_source,
            mkt_term,
            name_tracker,
            network_userid,
            os_family,
            os_manufacturer,
            os_name,
            os_timezone,
            page_referrer,
            page_title,
            page_url,
            page_urlfragment,
            page_urlhost,
            page_urlpath,
            page_urlport,
            page_urlquery,
            page_urlscheme,
            platform,
            try_to_numeric(pp_xoffset_max) as pp_xoffset_max,
            try_to_numeric(pp_xoffset_min) as pp_xoffset_min,
            try_to_numeric(pp_yoffset_max) as pp_yoffset_max,
            try_to_numeric(pp_yoffset_min) as pp_yoffset_min,
            refr_domain_userid,
            refr_dvce_tstamp,
            refr_medium,
            refr_source,
            refr_term,
            refr_urlfragment,
            refr_urlhost,
            refr_urlpath,
            refr_urlport,
            refr_urlquery,
            refr_urlscheme,
            se_action,
            se_category,
            se_label,
            se_property,
            se_value,
            ti_category,
            ti_currency,
            ti_name,
            ti_orderid,
            ti_price,
            ti_price_base,
            ti_quantity,
            ti_sku,
            tr_affiliation,
            tr_city,
            tr_country,
            tr_currency,
            tr_orderid,
            tr_shipping,
            tr_shipping_base,
            tr_state,
            tr_tax,
            tr_tax_base,
            tr_total,
            tr_total_base,
            true_tstamp,
            txn_id,
            unstruct_event,
            user_fingerprint,
            user_id,
            user_ipaddress,
            useragent,
            v_collector,
            v_etl,
            v_tracker,
            uploaded_at,
            'GitLab' as infra_source
        {% if target.name not in ("prod") -%}

            from {{ ref("snowplow_gitlab_good_events_sample_source") }}  -- The sample is not always from the current month so given then WHERE conditions this may be a blank tabel

        {%- else %} from {{ ref("snowplow_gitlab_good_events_source") }}

        {%- endif %}

        where
            app_id is not null
            and date_part(month, try_to_timestamp(derived_tstamp)) = '{{ month_value }}'
            and date_part(year, try_to_timestamp(derived_tstamp)) = '{{ year_value }}'
            and (
                (
                    -- js backend tracker
                    v_tracker like 'js%'
                    and lower(page_url) not like 'https://staging.gitlab.com/%'
                    and lower(page_url) not like 'https://customers.stg.gitlab.com/%'
                    and lower(page_url) not like 'http://localhost:%'
                )

                or

                (
                    -- ruby backend tracker
                    v_tracker like 'rb%'
                )
            )
            -- removing it after approval from @rparker2 in this issue:
            -- https://gitlab.com/gitlab-data/analytics/-/issues/9112
            and iff(
                event_name in ('submit_form', 'focus_form', 'change_form')
                and try_to_timestamp(derived_tstamp) < '2021-05-26',
                false,
                true
            )

    ),
    base as (

        select *
        from filtered_source fe1
        where
            not exists (
                select 1
                from filtered_source fe2
                where fe1.event_id = fe2.event_id
                group by event_id
                having count(*) > 1
            )

    ),
    events_with_web_page_id as (

        select * from {{ ref("snowplow_gitlab_events_web_page_id") }}

    ),
    events_with_standard_context as (

        select * from {{ ref("snowplow_gitlab_events_standard_context") }}

    ),
    base_with_sorted_columns as (

        select
            base.app_id,
            base.base_currency,
            base.br_colordepth,
            base.br_cookies,
            base.br_family,
            base.br_features_director,
            base.br_features_flash,
            base.br_features_gears,
            base.br_features_java,
            base.br_features_pdf,
            base.br_features_quicktime,
            base.br_features_realplayer,
            base.br_features_silverlight,
            base.br_features_windowsmedia,
            base.br_lang,
            base.br_name,
            base.br_renderengine,
            base.br_type,
            base.br_version,
            base.br_viewheight,
            base.br_viewwidth,
            base.collector_tstamp,
            base.contexts,
            base.derived_contexts,
            base.derived_tstamp,
            base.doc_charset,
            base.doc_height,
            base.doc_width,
            base.domain_sessionid,
            base.domain_sessionidx,
            base.domain_userid,
            base.dvce_created_tstamp,
            base.dvce_ismobile,
            base.dvce_screenheight,
            base.dvce_screenwidth,
            base.dvce_sent_tstamp,
            base.dvce_type,
            base.etl_tags,
            base.etl_tstamp,
            base.event,
            base.event_fingerprint,
            base.event_format,
            base.event_id,
            events_with_web_page_id.web_page_id,
            events_with_standard_context.environment as gsc_environment,
            events_with_standard_context.extra as gsc_extra,
            events_with_standard_context.namespace_id as gsc_namespace_id,
            events_with_standard_context.plan as gsc_plan,
            events_with_standard_context.google_analytics_client_id
            as gsc_google_analytics_client_id,
            events_with_standard_context.project_id as gsc_project_id,
            events_with_standard_context.pseudonymized_user_id
            as gsc_pseudonymized_user_id,
            events_with_standard_context.source as gsc_source,
            base.event_name,
            base.event_vendor,
            base.event_version,
            base.geo_city,
            base.geo_country,
            base.geo_latitude,
            base.geo_longitude,
            base.geo_region,
            base.geo_region_name,
            base.geo_timezone,
            base.geo_zipcode,
            base.ip_domain,
            base.ip_isp,
            base.ip_netspeed,
            base.ip_organization,
            base.mkt_campaign,
            base.mkt_clickid,
            base.mkt_content,
            base.mkt_medium,
            base.mkt_network,
            base.mkt_source,
            base.mkt_term,
            base.name_tracker,
            base.network_userid,
            base.os_family,
            base.os_manufacturer,
            base.os_name,
            base.os_timezone,
            base.page_referrer,
            base.page_title,
            base.page_url,
            base.page_urlfragment,
            base.page_urlhost,
            base.page_urlpath,
            base.page_urlport,
            base.page_urlquery,
            base.page_urlscheme,
            base.platform,
            base.pp_xoffset_max,
            base.pp_xoffset_min,
            base.pp_yoffset_max,
            base.pp_yoffset_min,
            base.refr_domain_userid,
            base.refr_dvce_tstamp,
            base.refr_medium,
            base.refr_source,
            base.refr_term,
            base.refr_urlfragment,
            base.refr_urlhost,
            base.refr_urlpath,
            base.refr_urlport,
            base.refr_urlquery,
            base.refr_urlscheme,
            base.se_action,
            base.se_category,
            base.se_label,
            base.se_property,
            base.se_value,
            base.ti_category,
            base.ti_currency,
            base.ti_name,
            base.ti_orderid,
            base.ti_price,
            base.ti_price_base,
            base.ti_quantity,
            base.ti_sku,
            base.tr_affiliation,
            base.tr_city,
            base.tr_country,
            base.tr_currency,
            base.tr_orderid,
            base.tr_shipping,
            base.tr_shipping_base,
            base.tr_state,
            base.tr_tax,
            base.tr_tax_base,
            base.tr_total,
            base.tr_total_base,
            base.true_tstamp,
            base.txn_id,
            base.unstruct_event,
            base.user_fingerprint,
            base.user_id,
            base.user_ipaddress,
            base.useragent,
            base.v_collector,
            base.v_etl,
            base.v_tracker,
            base.uploaded_at,
            base.infra_source
        from base
        left join
            events_with_web_page_id on base.event_id = events_with_web_page_id.event_id
        left join
            events_with_standard_context
            on base.event_id = events_with_standard_context.event_id
        where
            not exists (
                select event_id
                from events_with_web_page_id web_page_events
                where events_with_web_page_id.event_id = web_page_events.event_id
                group by event_id
                having count(1) > 1

            )

    ),
    unnested_unstruct as (

        select
            *,
            {{
                dbt_utils.get_url_parameter(
                    field="page_urlquery", url_parameter="glm_source"
                )
            }} as glm_source,
            case
                when
                    length(unstruct_event) > 0
                    and try_parse_json(unstruct_event) is null
                then true
                else false
            end as is_bad_unstruct_event,
            {{ unpack_unstructured_event(change_form, "change_form", "cf") }},
            {{ unpack_unstructured_event(submit_form, "submit_form", "sf") }},
            {{ unpack_unstructured_event(focus_form, "focus_form", "ff") }},
            {{ unpack_unstructured_event(link_click, "link_click", "lc") }},
            {{ unpack_unstructured_event(track_timing, "track_timing", "tt") }}
        from base_with_sorted_columns

    )

select *
from unnested_unstruct
order by derived_tstamp
