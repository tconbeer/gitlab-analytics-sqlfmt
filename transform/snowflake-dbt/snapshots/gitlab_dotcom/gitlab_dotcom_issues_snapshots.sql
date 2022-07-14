{% snapshot gitlab_dotcom_issues_snapshots %}

{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="updated_at",
    )
}}

select *
from {{ source("gitlab_dotcom", "issues") }}
qualify (row_number() OVER (partition by id order by updated_at desc) = 1)

{% endsnapshot %}
