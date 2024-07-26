{{ config({"materialized": "incremental", "unique_key": "credit_pk"}) }}

with
    source as (

        select *
        from {{ ref("gcp_billing_export_source") }}
        {% if is_incremental() %}

            where uploaded_at >= (select max(uploaded_at) from {{ this }})

        {% endif %}

    ),
    renamed as (

        select
            source.primary_key as source_primary_key,
            credits_flat.value['name']::varchar as credit_description,
            credits_flat.value['amount']::float as credit_amount,
            source.uploaded_at as uploaded_at,
            {{
                dbt_utils.surrogate_key(
                    ["source_primary_key", "credit_description", "credits_flat.value"]
                )
            }} as credit_pk
        from source, lateral flatten(input => credits) credits_flat

    )

select *
from renamed
