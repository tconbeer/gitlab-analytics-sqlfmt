
with
    source as (select * from {{ ref("gitlab_dotcom_plans_source") }}),
    renamed as (

        select

            plan_id as dim_plan_id,
            created_at,
            updated_at,
            plan_name,
            plan_title,
            plan_is_paid

        from source

    )

    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@mpeychet_",
            updated_by="@chrissharp",
            created_date="2021-05-30",
            updated_date="2022-02-10",
        )
    }}
