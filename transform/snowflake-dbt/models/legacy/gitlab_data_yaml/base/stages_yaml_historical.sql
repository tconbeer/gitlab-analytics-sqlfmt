with
    source as (select * from {{ ref("stages_yaml_source") }}),
    filtered as (select * from source where rank = 1)

select *
from filtered
