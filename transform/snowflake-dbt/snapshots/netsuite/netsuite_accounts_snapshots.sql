{% snapshot netsuite_accounts_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="account_id",
        updated_at="date_last_modified",
    )
}}

select *
from {{ source("netsuite", "accounts") }}

{% endsnapshot %}
