{% snapshot gitlab_dotcom_gitlab_subscriptions_namespace_id_snapshots %}

{{
    config(
        unique_key="namespace_id",
        strategy="check",
        check_cols=[
            "updated_at",
            "max_seats_used",
            "seats",
            "seats_in_use",
        ],
    )
}}

select *
from {{ source("gitlab_dotcom", "gitlab_subscriptions") }}
qualify row_number() OVER (partition by namespace_id order by updated_at desc) = 1

{% endsnapshot %}
