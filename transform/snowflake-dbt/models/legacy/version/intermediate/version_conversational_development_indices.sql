with
    base as (

        select * from {{ ref("version_conversational_development_indices_source") }}

    )

select *
from base
