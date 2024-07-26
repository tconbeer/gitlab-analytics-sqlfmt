with source as (select * from {{ ref("sfdc_quote_source") }} where is_deleted = false)

select *
from source
