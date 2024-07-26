with
    source as (

        select *
        from {{ ref("sfdc_account_source") }}
        where account_id is not null and is_deleted = false

    )
select *
from source
