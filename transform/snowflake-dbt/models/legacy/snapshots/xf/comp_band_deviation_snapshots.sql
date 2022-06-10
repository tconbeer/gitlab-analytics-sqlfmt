{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (

        select *
        from {{ ref("comp_band_loc_factor_base") }}

        UNION ALL

        select *
        from {{ ref("sheetload_comp_band_snapshot_base") }}

    )

select *
from source
