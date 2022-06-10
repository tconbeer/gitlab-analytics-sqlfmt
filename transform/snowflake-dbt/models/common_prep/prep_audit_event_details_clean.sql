with
    non_pii_details as (

        select audit_event_id, key_name, key_value, created_at
        from {{ ref("gitlab_dotcom_audit_event_details") }}
        where key_name != 'target_details'

    ),
    pii_details as (

        select audit_event_id, key_name, key_value_hash as key_value, created_at
        from {{ ref("gitlab_dotcom_audit_event_details_pii") }}

    ),
    unioned as (select * from non_pii_details UNION ALL select * from pii_details)

    {{
        dbt_audit(
            cte_ref="unioned",
            created_by="@ischweickartDD",
            updated_by="@ischweickartDD",
            created_date="2021-06-16",
            updated_date="2021-06-16",
        )
    }}
