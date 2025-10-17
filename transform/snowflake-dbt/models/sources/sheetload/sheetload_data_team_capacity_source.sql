with
    source as (select * from {{ source("sheetload", "data_team_capacity") }}),
    final as (

        select
            try_to_number(milestone_id) as milestone_id,
            nullif(gitlab_handle, '')::varchar as gitlab_handle,
            try_to_number(capacity) as capacity
        from source

    )

select *
from final
