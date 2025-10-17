with
    source as (

        select {{ hash_sensitive_columns("bizible_email_to_visitor_ids_source") }}
        from {{ ref("bizible_email_to_visitor_ids_source") }}

    )

select *
from source
