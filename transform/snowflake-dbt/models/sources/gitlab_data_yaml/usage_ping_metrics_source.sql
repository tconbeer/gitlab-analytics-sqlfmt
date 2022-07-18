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

    )

select *
from renamed
