{% snapshot gitlab_dotcom_projects_snapshots %}

{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="updated_at",
    )
}}

select *
from {{ source("gitlab_dotcom", "projects") }}
qualify (row_number() over (partition by id order by updated_at desc) = 1)

{% endsnapshot %}
