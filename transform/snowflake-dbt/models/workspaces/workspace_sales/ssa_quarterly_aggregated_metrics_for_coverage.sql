with
    base as (

        select *
        from {{ ref("driveload_ssa_quarterly_aggregated_metrics_for_coverage_source") }}

    )

select *
from base
