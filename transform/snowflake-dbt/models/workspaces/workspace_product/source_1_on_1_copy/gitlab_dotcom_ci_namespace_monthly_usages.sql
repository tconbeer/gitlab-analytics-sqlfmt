with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_namespace_monthly_usages_source") }}

    )

select *
from source
