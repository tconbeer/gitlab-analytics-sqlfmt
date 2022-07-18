with source as (select * from {{ ref("ga360_session_custom_dimension_source") }})

select *
from source
