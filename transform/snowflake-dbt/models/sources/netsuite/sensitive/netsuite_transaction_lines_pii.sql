with
    source as (select * from {{ ref("netsuite_transaction_lines_source") }}),
    transaction_lines_pii as (

        select
            transaction_lines_unique_id,
            {{ nohash_sensitive_columns("netsuite_transaction_lines_source", "memo") }}
        from source

    )

select *
from transaction_lines_pii
