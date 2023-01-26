with
    source as (select * from {{ ref("roles_yaml_source") }}),
    filtered as (select * from source where rank = 1)

select *
from filtered
