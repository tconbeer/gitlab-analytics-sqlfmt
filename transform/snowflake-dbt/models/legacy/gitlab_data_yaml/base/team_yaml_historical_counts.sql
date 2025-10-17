-- This file is loaded through dbt seed, your local runs will break unless you run dbt
-- seed first.
with source as (select * from {{ ref("historical_counts_maintainers_engineers") }})

select *
from source
