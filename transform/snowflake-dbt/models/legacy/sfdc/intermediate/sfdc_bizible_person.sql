with
    source as (

        select * from {{ ref("sfdc_bizible_person_source") }} where is_deleted = false

    )

select *
from source
