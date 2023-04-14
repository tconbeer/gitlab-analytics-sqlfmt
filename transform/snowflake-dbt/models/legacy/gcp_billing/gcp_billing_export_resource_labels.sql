{{ config({"materialized": "incremental", "unique_key": "resource_label_pk"}) }}

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
            resource_labels_flat.value['key']::varchar as resource_label_key,
            resource_labels_flat.value['value']::varchar as resource_label_value,
            source.uploaded_at as uploaded_at,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source_primary_key",
                        "resource_label_key",
                        "resource_label_value",
                    ]
                )
            }} as resource_label_pk
        from source, lateral flatten(input => labels) resource_labels_flat

    )

select *
from renamed
