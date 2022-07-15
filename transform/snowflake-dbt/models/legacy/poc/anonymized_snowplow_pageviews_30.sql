{%- set tables_to_import = [
    "configure_snowplow_smau_pageviews_events",
    "create_snowplow_smau_pageviews_events",
    "manage_snowplow_smau_pageviews_events",
    "monitor_snowplow_smau_pageviews_events",
    "package_snowplow_smau_pageviews_events",
    "plan_snowplow_smau_pageviews_events",
    "release_snowplow_smau_pageviews_events",
] -%}

{%- set fields_to_exclude = [
    "page_url",
    "page_url_path",
    "referer_url",
    "referer_url_path",
    "ip_address",
    "page_title",
] -%}

with
    snowplow_page_views_30 as (

        select
            {{
                dbt_utils.star(
                    from=ref("snowplow_page_views_30"),
                    except=fields_to_exclude | upper,
                )
            }}
        from {{ ref("snowplow_page_views_30") }}

    )

    {% for table_to_import in tables_to_import %}

    , {{ table_to_import }} as (select * from {{ ref(table_to_import) }})

    {% endfor -%},
    unioned as (

        {% for table_to_import in tables_to_import %}

        select *
        from {{ table_to_import }}

        {%- if not loop.last %}
        union
        {%- endif %}

        {% endfor -%}

    ),
    filtered_pageviews as (

        select snowplow_page_views_30.*, unioned.event_type as smau_event_type
        from snowplow_page_views_30
        inner join
            unioned on snowplow_page_views_30.page_view_id = unioned.event_surrogate_key

    )

select *
from filtered_pageviews
