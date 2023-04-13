with
    base as (select * from {{ source("gitlab_dotcom", "lfs_file_locks") }})

    {{ scd_latest_state(source="base", max_column="_task_instance") }}
