with
    source as (

        select *
        from {{ ref("categories_yaml_acquisitions_source") }}
        qualify
            row_number() over (
                partition by
                    category_name,
                    category_stage,
                    acquisition_key,
                    acquisition_name,
                    acquisition_end_date
                order by acquisition_start_date
            ) = 1

    ),
    final as (

        select
            category_name,
            category_stage,
            snapshot_date,
            acquisition_key,
            acquisition_name,
            acquisition_start_date,
            acquisition_end_date
        from source

    )

select *
from final
