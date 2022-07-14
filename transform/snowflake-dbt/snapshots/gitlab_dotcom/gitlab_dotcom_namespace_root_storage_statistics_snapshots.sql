{% snapshot gitlab_dotcom_namespace_root_storage_statistics_snapshots %}

{{
    config(
        unique_key="namespace_id",
        strategy="check",
        check_cols=[
            "repository_size",
            "lfs_objects_size",
            "wiki_size",
            "build_artifacts_size",
            "storage_size",
            "packages_size",
        ],
    )
}}

select *
from {{ source("gitlab_dotcom", "namespace_root_storage_statistics") }}
qualify row_number() OVER (partition by namespace_id order by _uploaded_at desc) = 1

{% endsnapshot %}
