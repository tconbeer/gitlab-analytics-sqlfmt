with
    location_region as (

        select dim_location_region_id, location_region_name
        from {{ ref("prep_location_region") }}
    )

    {{
        dbt_audit(
            cte_ref="location_region",
            created_by="@mcooperDD",
            updated_by="@mcooperDD",
            created_date="2020-12-29",
            updated_date="2020-12-29",
        )
    }}
