{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_issue_user_request AS (

    SELECT *
    FROM {{ ref('prep_issue_user_request') }}

), prep_issue_user_request_collaboration_project AS (

    SELECT *
    FROM {{ ref('prep_issue_user_request_collaboration_project') }}

), issue_request_collaboration_projects_filtered AS (

    -- Issue request that are in the collaboration projects but are not in the Gitlab-org issue descriptions or notes

    SELECT prep_issue_user_request_collaboration_project.*
    FROM prep_issue_user_request_collaboration_project
    LEFT JOIN prep_issue_user_request
      ON prep_issue_user_request.dim_issue_id = prep_issue_user_request_collaboration_project.dim_issue_id
      AND prep_issue_user_request.dim_crm_account_id = prep_issue_user_request_collaboration_project.dim_crm_account_id
    WHERE prep_issue_user_request.dim_issue_id IS NULL

), unioned AS (


    SELECT
      dim_issue_id,
      link_type,
      dim_crm_opportunity_id,
      dim_crm_account_id,
      dim_ticket_id,
      request_priority,
      FALSE                 AS is_user_request_only_in_collaboration_project 
    FROM prep_issue_user_request

    UNION

    SELECT
      dim_issue_id,
      'Account'             AS link_type,
      MD5(-1)               AS dim_crm_opportunity_id,
      dim_crm_account_id,
      -1                    AS dim_ticket_id,
      1::NUMBER             AS request_priority,
      TRUE                  AS is_user_request_only_in_collaboration_project
    FROM issue_request_collaboration_projects_filtered

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-10-12",
    updated_date="2021-10-12",
) }}

