with
    source as (

        select *
        from {{ ref("sfdc_bizible_attribution_touchpoint_source") }}
        where is_deleted = false

    )

select *
from source
