with
    base as (select * from {{ source("gitlab_dotcom", "bulk_import_entities") }})

    {{ scd_latest_state(source="base", max_column="_task_instance") }}
