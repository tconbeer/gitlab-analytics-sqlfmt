with
    source as (select * from {{ ref("location_factors_yaml_flatten_source") }}),
    organized as (

        select
            valid_from,
            valid_to,
            is_current,
            country_level_1 as country,
            -- Idenitfy type based on where the data is from the flattend structure.
            case
                when area_level_1 is not null
                then 'type_1'
                when states_or_provinces_metro_areas_name_level_2 is not null
                then 'type_2'
                when metro_areas_sub_location_level_2 is not null
                then 'type_3'
                when
                    states_or_provinces_metro_areas_name_level_2 is null
                    and states_or_provinces_name_level_2 is not null
                then 'type_4'
                when
                    metro_areas_sub_location_level_2 is null
                    and metro_areas_name_level_2 is not null
                then 'type_5'
                else 'type_6'
            end as type,
            case
                type
                when 'type_1'
                then area_level_1
                when 'type_2'
                then
                    states_or_provinces_metro_areas_name_level_2
                    || ', '
                    || states_or_provinces_name_level_2
                when 'type_3'
                then
                    metro_areas_name_level_2 || ', ' || metro_areas_sub_location_level_2
                when 'type_4'
                then states_or_provinces_name_level_2
                when 'type_5'
                then metro_areas_name_level_2
                else null
            end as area_raw,
            listagg(distinct area_raw, ',') over (partition by country) as area_list,
            -- Prefer All to Everyere else for areas without as it is what is currentl
            -- added in the application.
            case
                when contains(area_list, 'All')
                then 'All'
                when contains(area_list, 'Everywhere else')
                then 'Everywhere else'
                else null
            end as other_area,
            -- Cleaning basic spelling erros in the source data and apply a derived
            -- area prefix if there is none.
            case
                when area_raw is null
                then other_area
                when contains(area_raw, 'Zurig')
                then regexp_replace(area_raw, 'Zurig', 'Zurich')
                when contains(area_raw, 'Edinbugh')
                then regexp_replace(area_raw, 'Edinbugh', 'Edinburgh')
                else area_raw
            end as area_clean,
            -- Adjusting factor to match what is found in the geozone source data
            coalesce(
                states_or_provinces_metro_areas_factor_level_2,
                states_or_provinces_factor_level_2,
                metro_areas_factor_level_2,
                factor_level_1,
                locationfactor_level_1
            )
            * 0.01 as factor
        from source

    ),
    grouping as (

        select
            country,
            area_clean as area,
            area || ', ' || country as locality,
            factor,
            valid_from,
            valid_to,
            is_current,
            /* Filling in NULLs with a value for the inequality check in the next step of the gaps and islands problem
      (finding groups based on when the factor changes and not just the value of the factor)
      */
            lag(factor, 1, 0) over (
                partition by locality order by valid_from
            ) as lag_factor,
            conditional_true_event(factor != lag_factor) over (
                partition by locality order by valid_from
            ) as locality_group,
            lead(valid_from, 1) over (
                partition by locality order by valid_from
            ) as next_entry
        from organized

    ),
    final as (

        select distinct
            country as location_factor_country,
            area as location_factor_area,
            locality,
            factor as location_factor,
            min(valid_from) over (
                partition by locality, locality_group
            )::date as first_file_date,
            max(valid_to) over (
                partition by locality, locality_group
            )::date as last_file_date,
            -- Fixed date represents when location factor becan to be collected in
            -- source data.
            iff(
                locality_group = 1,
                least('2020-03-24', first_file_date),
                first_file_date
            ) as valid_from,
            max(coalesce(next_entry,{{ var("tomorrow") }})) over (
                partition by locality, locality_group
            )::date as valid_to,
            boolor_agg(is_current) over (
                partition by locality, locality_group
            ) as is_current_file
        from grouping
        where factor is not null

    )

select *
from final
