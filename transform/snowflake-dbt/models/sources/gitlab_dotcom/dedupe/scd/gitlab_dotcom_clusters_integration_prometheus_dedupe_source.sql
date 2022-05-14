with
    base as (

        select * from {{ source("gitlab_dotcom", "clusters_integration_prometheus") }}

    )

    {{ scd_latest_state(source="base", max_column="_task_instance") }}
