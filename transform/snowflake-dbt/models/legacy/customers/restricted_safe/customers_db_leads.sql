with
    source as (

        select {{ hash_sensitive_columns("customers_db_leads_source") }}
        from {{ ref("customers_db_leads_source") }}

    )

select *
from source
