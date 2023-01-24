{% snapshot netsuite_departments_snapshots %}

{{
    config(
        strategy="timestamp",
        unique_key="department_id",
        updated_at="date_last_modified",
    )
}}

select *
from {{ source("netsuite", "departments") }}

{% endsnapshot %}
