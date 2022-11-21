with
    source as (select * from {{ source("sheetload", "usage_ping_metrics_sections") }}),
    renamed as (

        select
            section::varchar as section_name,
            metrics_path::varchar as metrics_path,
            stage::varchar as stage_name,
            "group"::varchar as group_name,
            smau::boolean as is_smau,
            gmau::boolean as is_gmau,
            clean_metric_name::varchar as clean_metrics_name,
            periscope_metrics_name::varchar as periscope_metrics_name,
            time_period::varchar as time_period,
            mau::boolean as is_umau,
            paid_gmau::boolean as is_paid_gmau
        from source

    )

select *
from renamed
