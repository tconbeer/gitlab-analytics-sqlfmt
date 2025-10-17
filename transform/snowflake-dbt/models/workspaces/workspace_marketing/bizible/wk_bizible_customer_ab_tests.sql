with
    source as (

        select {{ hash_sensitive_columns("bizible_customer_ab_tests_source") }}
        from {{ ref("bizible_customer_ab_tests_source") }}

    )

select *
from source
