{{ config(tags=["mnpi_exception"]) }}

with
    prep_epic_user_request as (select * from {{ ref("prep_epic_user_request") }}),
    prep_epic_user_request_collaboration_project as (

        select * from {{ ref("prep_epic_user_request_collaboration_project") }}

    ),
    epic_request_collaboration_projects_filtered as (

        -- Epic request that are in the collaboration projects but are not in the
        -- Gitlab-org issue descriptions or notes
        select prep_epic_user_request_collaboration_project.*
        from prep_epic_user_request_collaboration_project
        left join
            prep_epic_user_request
            on prep_epic_user_request.dim_epic_id
            = prep_epic_user_request_collaboration_project.dim_epic_id
            and prep_epic_user_request.dim_crm_account_id
            = prep_epic_user_request_collaboration_project.dim_crm_account_id
        where prep_epic_user_request.dim_epic_id is null

    ),
    unioned as (

        select
            dim_epic_id,
            link_type,
            dim_crm_opportunity_id,
            dim_crm_account_id,
            dim_ticket_id,
            request_priority,
            is_request_priority_empty,
            false as is_user_request_only_in_collaboration_project,
            link_last_updated_at
        from prep_epic_user_request

        UNION

        select
            dim_epic_id,
            'Account' as link_type,
            md5(-1) as dim_crm_opportunity_id,
            dim_crm_account_id,
            -1 as dim_ticket_id,
            1::number as request_priority,
            true as is_request_priority_empty,
            true as is_user_request_only_in_collaboration_project,
            link_last_updated_at
        from epic_request_collaboration_projects_filtered

    )

    {{
        dbt_audit(
            cte_ref="unioned",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-10-12",
            updated_date="2021-11-16",
        )
    }}
