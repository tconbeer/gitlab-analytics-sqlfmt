with
    source as (

        select * from {{ ref("gitlab_dotcom_design_management_versions_source") }}

    )

select *
from source
