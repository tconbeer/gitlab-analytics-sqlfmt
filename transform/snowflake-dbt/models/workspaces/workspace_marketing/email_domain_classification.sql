with base as (select * from {{ ref("driveload_email_domain_classification_source") }})

select *
from base
