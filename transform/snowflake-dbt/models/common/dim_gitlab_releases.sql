with
    base as (

        select *
        from {{ ref("release_managers_source") }}
        qualify
            row_number() over (
                partition by major_minor_version order by snapshot_date desc, rank desc
            ) = 1

    )

    {{
        dbt_audit(
            cte_ref="base",
            created_by="@mpeychet",
            updated_by="@mpeychet",
            created_date="2021-05-03",
            updated_date="2021-05-03",
        )
    }}
