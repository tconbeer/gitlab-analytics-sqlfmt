with
    base as (select * from {{ source("gitlab_dotcom", "group_import_states") }})

    {{ scd_latest_state(source="base", max_column="_task_instance") }}
