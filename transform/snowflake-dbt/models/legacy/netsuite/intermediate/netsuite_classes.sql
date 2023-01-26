with source as (select * from {{ ref("netsuite_classes_source") }})

select *
from source
where is_fivetran_deleted = false
