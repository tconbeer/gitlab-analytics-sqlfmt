{% snapshot gitlab_dotcom_namespaces_snapshots %}

    {{
        config(
            unique_key="id",
            strategy="timestamp",
            updated_at="updated_at",
        )
    }}

    select *
    from {{ source("gitlab_dotcom", "namespaces") }}
    qualify (row_number() over (partition by id order by updated_at desc) = 1)

{% endsnapshot %}
