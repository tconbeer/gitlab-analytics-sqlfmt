{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}


with
    source as (select * from {{ source("gainsight", "gainsight_instance_info") }}),
    final as (

        select
            crm_acct_id::varchar as crm_account_id,
            gainsight_unique_row_id::varchar as gainsight_unique_row_id,
            instance_uuid::varchar as instance_uuid,
            hostname::varchar as instance_hostname,
            instancetype::varchar as instance_type,
            "Namespace_ID"::varchar as namespace_id,
            to_timestamp(_updated_at::number) as uploaded_at
        from source

    )

select *
from final
