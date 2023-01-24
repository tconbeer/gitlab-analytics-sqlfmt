{% snapshot netsuite_transactions_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="transaction_id",
        updated_at="date_last_modified",
    )
}}

select *
from {{ source("netsuite", "transactions") }}

{% endsnapshot %}
