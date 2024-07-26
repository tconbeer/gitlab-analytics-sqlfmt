with
    source as (select * from {{ ref("feature_flags_source") }}),
    filtered as (select * from source where rank = 1)

select *
from filtered
