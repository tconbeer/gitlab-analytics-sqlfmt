with
    base as (

        select *
        from {{ ref("sfdc_opportunity_source") }}
        where account_id is not null and is_deleted = false

    )

select *
from base
