with
    environment as (

        select 1 as dim_environment_id, 'Gitlab.com' as environment

        union

        select 2 as dim_environment_id, 'License DB' as environment

        union

        select 3 as dim_environment_id, 'Customers Portal' as environment

    )

    {{
        dbt_audit(
            cte_ref="environment",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-09-22",
            updated_date="2021-09-22",
        )
    }}
