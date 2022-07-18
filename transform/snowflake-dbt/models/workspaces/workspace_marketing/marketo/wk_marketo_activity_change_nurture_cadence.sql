with
    source as (

        select * from {{ ref("marketo_activity_change_nurture_cadence_source") }}

    )

select *
from source
