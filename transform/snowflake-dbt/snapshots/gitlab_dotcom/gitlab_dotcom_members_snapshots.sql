{% snapshot gitlab_dotcom_members_snapshots %}

{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="created_at",
    )
}}

select *
from {{ source("gitlab_dotcom", "members") }}
qualify (row_number() OVER (partition by id order by _uploaded_at desc) = 1)

{% endsnapshot %}
