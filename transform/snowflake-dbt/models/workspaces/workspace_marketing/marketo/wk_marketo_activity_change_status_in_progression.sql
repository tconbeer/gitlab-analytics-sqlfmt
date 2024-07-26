with
    source as (

        select * from {{ ref("marketo_activity_change_status_in_progression_source") }}

    )

select *
from source
