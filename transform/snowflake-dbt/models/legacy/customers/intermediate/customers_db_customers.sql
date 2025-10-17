with
    source as (

        select {{ hash_sensitive_columns("customers_db_customers_source") }}
        from {{ ref("customers_db_customers_source") }}

    )

select *
from source
