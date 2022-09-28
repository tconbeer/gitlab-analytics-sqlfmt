with base as (select * from {{ source("snapshots", "fct_mrr_snapshot") }})

select *
from base
