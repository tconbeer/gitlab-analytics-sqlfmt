with
    source as (

        select {{ hash_sensitive_columns("netsuite_transaction_lines_source") }}
        from {{ ref("netsuite_transaction_lines_source") }}

    )

select *
from source
where non_posting_line != 'yes'  -- removes transactions not intended for posting
