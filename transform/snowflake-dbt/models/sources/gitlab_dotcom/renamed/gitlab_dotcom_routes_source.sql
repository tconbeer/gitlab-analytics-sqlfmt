with
    gitlab_dotcom_routes as (

        select * from {{ ref("gitlab_dotcom_routes_dedupe_source") }}

    )

select
    id::number as route_id,
    source_id::number as source_id,
    source_type::varchar as source_type,
    path::varchar as path
from gitlab_dotcom_routes
