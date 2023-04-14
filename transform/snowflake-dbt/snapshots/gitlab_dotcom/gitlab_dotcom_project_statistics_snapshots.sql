{% snapshot gitlab_dotcom_project_statistics_snapshots %}

    {{
        config(
            unique_key="id",
            strategy="check",
            check_cols=[
                "repository_size",
                "commit_count",
                "storage_size",
                "repository_size",
                "lfs_objects_size",
                "packages_size",
                "wiki_size",
                "build_artifacts_size",
                "shared_runners_seconds",
                "shared_runners_seconds_last_reset",
            ],
        )
    }}

    select *
    from {{ source("gitlab_dotcom", "project_statistics") }}
    qualify row_number() over (partition by id order by _uploaded_at desc) = 1

{% endsnapshot %}
