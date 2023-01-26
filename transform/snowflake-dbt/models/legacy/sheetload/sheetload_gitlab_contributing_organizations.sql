with
    source as (

        select * from {{ ref("sheetload_gitlab_contributing_organizations_source") }}

    )

select *
from source
