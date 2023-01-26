with
    source as (select * from {{ ref("customers_db_customers_source") }}),
    customers_db_customers_pii as (

        select
            customer_id,
            {{
                nohash_sensitive_columns(
                    "customers_db_customers_source", "customer_email"
                )
            }}
        from source

    )

select *
from customers_db_customers_pii
