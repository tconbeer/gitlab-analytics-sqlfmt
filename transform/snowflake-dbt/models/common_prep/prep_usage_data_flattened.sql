{{ config(tags=["product", "mnpi_exception"]) }}


{{ config({"materialized": "incremental", "unique_key": "instance_path_id"}) }}

with
    data as (

        select *
        from {{ ref("prep_usage_ping_payload") }}
        {% if is_incremental() %}

            where dim_date_id >= (select max(dim_date_id) from {{ this }})

        {% endif %}

    ),
    flattened as (

        select
            {{ dbt_utils.surrogate_key(["dim_usage_ping_id", "path"]) }}
            as instance_path_id,
            dim_usage_ping_id,
            dim_date_id,
            path as metrics_path,
            value as metric_value
        from data, lateral flatten(input => raw_usage_data_payload, recursive => true)

    )

    {{
        dbt_audit(
            cte_ref="flattened",
            created_by="@mpeychet",
            updated_by="@mpeychet",
            created_date="2021-07-21",
            updated_date="2021-07-21",
        )
    }}
