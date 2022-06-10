with
    base as (

        select * from {{ source("snapshots", "mart_available_to_renew_snapshot") }}

    )

select *
from base
