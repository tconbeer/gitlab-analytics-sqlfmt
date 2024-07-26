with
    source as (

        select *
        from {{ ref("thanos_stage_group_error_budget_availability_source") }}
        where is_success = true
    )
select *
from source
