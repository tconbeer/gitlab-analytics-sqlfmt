{{ config(tags=["mnpi_exception"]) }}

with
    prep_issue_user_request as (select * from {{ ref("prep_issue_user_request") }}),
    prep_issue_user_request_collaboration_project as (

        select * from {{ ref("prep_issue_user_request_collaboration_project") }}

    ),
    issue_request_collaboration_projects_filtered as (

        -- Issue request that are in the collaboration projects but are not in the
        -- Gitlab-org issue descriptions or notes
        select prep_issue_user_request_collaboration_project.*
        from prep_issue_user_request_collaboration_project
        left join
            prep_issue_user_request
            on prep_issue_user_request.dim_issue_id
            = prep_issue_user_request_collaboration_project.dim_issue_id
            and prep_issue_user_request.dim_crm_account_id
            = prep_issue_user_request_collaboration_project.dim_crm_account_id
        where prep_issue_user_request.dim_issue_id is null

    ),
    unioned as (

        select
            dim_issue_id,
            link_type,
            dim_crm_opportunity_id,
            dim_crm_account_id,
            dim_ticket_id,
            request_priority,
            is_request_priority_empty,
            false as is_user_request_only_in_collaboration_project,
            link_last_updated_at
        from prep_issue_user_request

        union

        select
            dim_issue_id,
            'Account' as link_type,
            md5(-1) as dim_crm_opportunity_id,
            dim_crm_account_id,
            -1 as dim_ticket_id,
            1::number as request_priority,
            true as is_request_priority_empty,
            true as is_user_request_only_in_collaboration_project,
            link_last_updated_at
        from issue_request_collaboration_projects_filtered

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
