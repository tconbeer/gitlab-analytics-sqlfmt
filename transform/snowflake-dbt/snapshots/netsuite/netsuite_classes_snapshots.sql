{% snapshot netsuite_classes_snapshots %}

    {{
        config(
            strategy="timestamp",
            unique_key="class_id",
            updated_at="date_last_modified",
        )
    }}

    select *
    from {{ source("netsuite", "classes") }}

{% endsnapshot %}
