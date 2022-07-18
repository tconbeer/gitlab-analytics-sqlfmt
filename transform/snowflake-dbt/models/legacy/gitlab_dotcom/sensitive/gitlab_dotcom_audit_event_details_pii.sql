{{ config({"materialized": "table"}) }}

with
    base as (select * from {{ ref("gitlab_dotcom_audit_event_details") }}),
    audit_event_pii as (

        select
            audit_event_id,
            key_name,
            {{
                nohash_sensitive_columns(
                    "gitlab_dotcom_audit_event_details", "key_value"
                )
            }},
            created_at
        from base
        where key_name = 'target_details'

    )

select *
from audit_event_pii
