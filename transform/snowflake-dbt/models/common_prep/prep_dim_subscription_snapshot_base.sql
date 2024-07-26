with base as (select * from {{ source("snapshots", "dim_subscription_snapshot") }})

select *
from base
