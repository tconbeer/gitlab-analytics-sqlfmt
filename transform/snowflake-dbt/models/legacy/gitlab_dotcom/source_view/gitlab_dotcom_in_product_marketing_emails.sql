with
    source as (

        select * from {{ ref("gitlab_dotcom_in_product_marketing_emails_source") }}


    )

select *
from source
