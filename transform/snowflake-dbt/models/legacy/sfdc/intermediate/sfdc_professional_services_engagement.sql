with
    source as (
        select *
        from {{ ref("sfdc_statement_of_work_source") }}
        where is_deleted = false
    )

select *
from source
