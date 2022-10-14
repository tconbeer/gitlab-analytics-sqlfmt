{% snapshot gitlab_dotcom_application_settings_snapshots %}

{{
    config(
        unique_key="id",
        strategy="check",
        check_cols=["shared_runners_minutes", "repository_size_limit"],
    )
}}

select *
from {{ source("gitlab_dotcom", "application_settings") }}
qualify row_number() over (partition by id order by _uploaded_at desc) = 1

{% endsnapshot %}
