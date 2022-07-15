with
    source as (

        select * from {{ ref("gitlab_dotcom_gitlab_subscription_histories_source") }}

    )

select *
from source
