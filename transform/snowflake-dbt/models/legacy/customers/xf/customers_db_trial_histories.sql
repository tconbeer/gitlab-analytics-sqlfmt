with source as (select * from {{ ref("customers_db_trial_histories_source") }})

select *
from source
