{{
    config(
        {
            "materialized": "view",
        }
    )
}}

with
    final as (

        select 6543 as ultimate_parent_namespace_id
        UNION ALL
        select 9970
        UNION ALL
        select 4347861
        UNION ALL
        select 1400979
        UNION ALL
        select 2299361
        UNION ALL
        select 1353442
        UNION ALL
        select 349181
        UNION ALL
        select 3455548
        UNION ALL
        select 3068744
        UNION ALL
        select 5362395
        UNION ALL
        select 4436569
        UNION ALL
        select 3630110
        UNION ALL
        select 3315282
        UNION ALL
        select 5811832
        UNION ALL
        select 5496509
        UNION ALL
        select 4206656
        UNION ALL
        select 5495265
        UNION ALL
        select 5496484
        UNION ALL
        select 2524164
        UNION ALL
        select 4909902

    )


    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@snalamaru",
            created_date="2020-12-29",
            updated_date="2020-12-29",
        )
    }}
