with source as (select * from {{ source("driveload", "email_domain_classification") }})

select domain::varchar as domain, classification::varchar as classification
from source
