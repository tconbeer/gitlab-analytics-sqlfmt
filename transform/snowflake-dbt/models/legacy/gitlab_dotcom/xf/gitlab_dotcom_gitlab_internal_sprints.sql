with
    sprints_source as (select * from {{ ref("gitlab_dotcom_sprints_source") }}),
    internal_groups as (

        select * from {{ ref("gitlab_dotcom_groups_xf") }} where group_is_internal

    ),
    filtered_sprints as (

        select *
        from sprints_source
        where
            exists (
                select 1
                from internal_groups
                where sprints_source.group_id = internal_groups.group_id
            )

    )

select *
from filtered_sprints
