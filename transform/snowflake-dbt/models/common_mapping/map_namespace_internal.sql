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
        union all
        select 9970
        union all
        select 4347861
        union all
        select 1400979
        union all
        select 2299361
        union all
        select 1353442
        union all
        select 349181
        union all
        select 3455548
        union all
        select 3068744
        union all
        select 5362395
        union all
        select 4436569
        union all
        select 3630110
        union all
        select 3315282
        union all
        select 5811832
        union all
        select 5496509
        union all
        select 4206656
        union all
        select 5495265
        union all
        select 5496484
        union all
        select 2524164
        union all
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
