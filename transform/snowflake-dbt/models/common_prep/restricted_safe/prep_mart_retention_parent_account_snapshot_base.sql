with
    base as (

        select *
        from {{ source("snapshots", "mart_retention_parent_account_snapshot") }}

    )

select *
from base
