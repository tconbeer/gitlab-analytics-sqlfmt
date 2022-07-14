{{ config(tags=["product", "mnpi_exception"], materialized="table") }}


with
    source as (select * from {{ source("gitlab_data_yaml", "usage_ping_metrics") }}),
    intermediate as (

        select
            d.value as data_by_row,
            uploaded_at,
            date_trunc('day', uploaded_at)::date as snapshot_date
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['key_path']::text as metrics_path,
            data_by_row['data_source']::text as data_source,
            data_by_row['description']::text as description,
            data_by_row['product_category']::text as product_category,
            data_by_row['product_group']::text as product_group,
            data_by_row['product_section']::text as product_section,
            data_by_row['product_stage']::text as product_stage,
            data_by_row['milestone']::text as milestone,
            data_by_row['skip_validation']::text as skip_validation,
            data_by_row['status']::text as metrics_status,
            data_by_row['tier'] as tier,
            data_by_row['time_frame']::text as time_frame,
            data_by_row['value_type']::text as value_type,
            array_contains(
                'gmau'::variant, data_by_row['performance_indicator_type']
            ) as is_gmau,
            array_contains(
                'smau'::variant, data_by_row['performance_indicator_type']
            ) as is_smau,
            array_contains(
                'paid_gmau'::variant, data_by_row['performance_indicator_type']
            ) as is_paid_gmau,
            array_contains(
                'umau'::variant, data_by_row['performance_indicator_type']
            ) as is_umau,
            snapshot_date,
            uploaded_at
        from intermediate

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["metrics_path"]) }}
            as dim_service_ping_metric_id,
            metrics_path as metrics_path,
            data_source as data_source,
            description as description,
            product_category as product_category,
            iff(
                substring(product_group, 0, 5) = 'group',
                split_part(replace(product_group, ' ', '_'), ':', 3),
                replace(product_group, ' ', '_')
            ) as group_name,
            product_section as section_name,
            product_stage as stage_name,
            milestone as milestone,
            skip_validation as skip_validation,
            metrics_status as metrics_status,
            tier as tier,
            time_frame as time_frame,
            value_type as value_type,
            is_gmau as is_gmau,
            is_smau as is_smau,
            is_paid_gmau as is_paid_gmau,
            is_umau as is_umau,
            snapshot_date as snapshot_date,
            uploaded_at as uploaded_at
        from renamed
        qualify max(uploaded_at) OVER () = uploaded_at

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@icooper-acp",
            updated_by="@icooper-acp",
            created_date="2022-04-14",
            updated_date="2022-04-14",
        )
    }}
