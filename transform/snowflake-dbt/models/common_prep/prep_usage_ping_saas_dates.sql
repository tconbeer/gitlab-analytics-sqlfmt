with
    base as (select * from {{ ref("prep_usage_ping") }} where ping_source = 'SaaS'),
    saas_pings as (

        select
            dim_usage_ping_id,
            ping_created_at_date,
            ping_created_at_28_days_earlier,
            ping_created_at_year,
            ping_created_at_month,
            ping_created_at_week
        from base

    ),
    final as (

        select *
        from saas_pings
        qualify
            row_number() OVER (
                partition by ping_created_at_date order by dim_usage_ping_id desc
            )
            = 1

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@kathleentam",
            updated_by="@ischweickartDD",
            created_date="2021-01-11",
            updated_date="2021-04-05",
        )
    }}
