{{ config({"materialized": "incremental"}) }}

with
    source as (

        select *
        from {{ ref("gitlab_dotcom_audit_events_source") }}
        {% if is_incremental() %}
        where created_at >= (select max(created_at) from {{ this }}) {% endif %}

    ),
    sequence as ({{ dbt_utils.generate_series(upper_bound=11) }}),
    details_parsed as (

        select
            audit_event_id,
            regexp_substr(
                audit_event_details, '\\:([a-z_]*)\\: (.*)', 1, generated_number, 'c', 1
            ) as key_name,
            regexp_substr(
                audit_event_details, '\\:([a-z_]*)\\: (.*)', 1, generated_number, 'c', 2
            ) as key_value,
            created_at
        from source
        inner join sequence
        where key_name is not null

    )

select *
from details_parsed
