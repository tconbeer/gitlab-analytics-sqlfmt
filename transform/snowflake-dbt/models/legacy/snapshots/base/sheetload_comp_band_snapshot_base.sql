{{ config({"materialized": "ephemeral"}) }}

with
    source as (

        select * from {{ source("snapshots", "sheetload_comp_band_snapshots") }}

    ),
    renamed as (

        select
            employee_number,
            percent_over_top_end_of_band,
            case
                when nullif(lower(percent_over_top_end_of_band), '') = 'exec'
                then 0.00
                when nullif(percent_over_top_end_of_band, '') = '#DIV/0!'
                then null
                when percent_over_top_end_of_band like '%'
                then nullif(replace(percent_over_top_end_of_band, '%', ''), '')
                else nullif(percent_over_top_end_of_band, '')
            end as percent_over_top_end_of_band_cleaned,
            dbt_valid_from::date as valid_from,
            dbt_valid_to::date as valid_to
        from source
        where percent_over_top_end_of_band is not null

    ),
    deduplicated as (

        select distinct
            employee_number,
            percent_over_top_end_of_band as original_value,
            iff(
                contains(percent_over_top_end_of_band, '%') = true,
                round(percent_over_top_end_of_band_cleaned / 100::float, 4),
                round(percent_over_top_end_of_band_cleaned::float, 4)
            ) as deviation_from_comp_calc,
            valid_from,
            valid_to
        from renamed

    ),
    final as (

        select
            employee_number,
            original_value,
            deviation_from_comp_calc,
            min(valid_from) as valid_from,
            nullif(max(valid_to), current_date) as valid_to
        from deduplicated
        group by 1, 2, 3

    )

select *
from final
