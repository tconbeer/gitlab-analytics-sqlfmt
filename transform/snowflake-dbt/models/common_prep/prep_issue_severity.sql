{{ config(tags=["product"]) }}


with
    gitlab_dotcom_issue_severity_source as (

        select * from {{ ref("gitlab_dotcom_issuable_severities_source") }}

    ),
    renamed as (

        select
            gitlab_dotcom_issue_severity_source.issue_severity_id
            as dim_issue_severity_id,
            gitlab_dotcom_issue_severity_source.issue_id as dim_issue_id,
            gitlab_dotcom_issue_severity_source.severity as severity
        from gitlab_dotcom_issue_severity_source

    )

    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@dtownsend",
            updated_by="@dtownsend",
            created_date="2021-08-04",
            updated_date="2021-08-04",
        )
    }}
