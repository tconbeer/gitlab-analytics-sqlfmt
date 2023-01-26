with
    base as (select * from {{ source("gitlab_dotcom", "path_locks") }})

    {{ scd_latest_state(source="base", max_column="_task_instance") }}
