{{ config({"materialized": "incremental", "unique_key": "event_surrogate_key"}) }}

with
    snowplow_structured_events as (

        select
            user_snowplow_domain_id,
            user_custom_id,
            derived_tstamp,
            page_url_path,
            event_id,
            event_action,
            event_label
        from {{ ref("snowplow_structured_events") }}
        where
            derived_tstamp >= '2019-01-01'
            {% if is_incremental() %}
            and derived_tstamp >= (select max({{ this }}.event_date) from {{ this }})
            {% endif %}
            and (
                event_action in (
                    'delete_repository',
                    'delete_tag',
                    'delete_tag_bulk',
                    'list_repositories',
                    'list_tags'
                )

                or

                event_label in (
                    'bulk_registry_tag_delete',
                    'registry_repository_delete',
                    'registry_tag_delete'

                )

            )

    ),
    renamed as (

        select
            user_snowplow_domain_id,
            user_custom_id,
            to_date(derived_tstamp) as event_date,
            page_url_path,
            event_action
            || iff(event_label is not null, '_' || event_label, null) as event_type,
            event_id as event_surrogate_key
        from snowplow_structured_events

    )

select *
from renamed
