{% snapshot gitlab_dotcom_namespace_statistics_snapshots %}

{{
    config(
        unique_key="id",
        strategy="check",
        check_cols=["shared_runners_seconds", "shared_runners_seconds_last_reset"],
    )
}}

select *
from {{ source("gitlab_dotcom", "namespace_statistics") }}
qualify row_number() OVER (partition by id order by _uploaded_at desc) = 1

{% endsnapshot %}
