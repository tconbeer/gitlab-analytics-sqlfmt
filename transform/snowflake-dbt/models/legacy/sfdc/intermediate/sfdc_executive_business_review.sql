with
    source as (

        select *
        from {{ ref("sfdc_executive_business_review_source") }}
        where is_deleted = false

    )

select *
from source
