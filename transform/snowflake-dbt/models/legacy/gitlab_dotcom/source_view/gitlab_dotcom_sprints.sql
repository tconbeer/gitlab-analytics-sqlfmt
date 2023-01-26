with
    source as (

        select
            {{
                dbt_utils.star(
                    from=ref("gitlab_dotcom_sprints_source"),
                    except=[
                        "SPRINT_TITLE",
                        "SPRINT_TITLE_HTML",
                        "SPRINT_DESCRIPTION",
                        "SPRINT_DESCRIPTION_HTML",
                    ],
                )
            }}
        from {{ ref("gitlab_dotcom_sprints_source") }}

    )

select *
from source
