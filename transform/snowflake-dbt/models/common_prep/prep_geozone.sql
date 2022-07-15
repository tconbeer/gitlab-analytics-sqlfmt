with
    source as (select * from {{ ref("geozones_yaml_flatten_source") }}),
    grouping as (

        select
            geozone_title,
            iff(
                country = 'United States',
                geozone_title || ', ' || country,
                geozone_title
            ) as geozone_locality,
            'All, ' || country as country_locality,
            geozone_factor,
            country,
            state_or_province,
            valid_from,
            valid_to,
            is_current,
            /* Filling in NULLs with a value for the inequality check in the next step of the gaps and islands problem
      (finding groups based on when the factor changes and not just the value of the factor)
      */
            lag(geozone_factor, 1, 0) over (
                partition by country, state_or_province order by valid_from
            ) as lag_factor,
            conditional_true_event(geozone_factor != lag_factor) over (
                partition by country, state_or_province order by valid_from
            ) as locality_group,
            lead(valid_from, 1) over (
                partition by country, state_or_province order by valid_from
            ) as next_entry
        from source

    ),
    final as (

        select distinct
            geozone_title,
            geozone_factor,
            geozone_locality,
            country_locality,
            country as geozone_country,
            state_or_province as geozone_state_or_province,
            min(valid_from) over (
                partition by country, state_or_province, locality_group
            )::date as first_file_date,
            max(valid_to) over (
                partition by country, state_or_province, locality_group
            )::date as last_file_date,
            -- Fixed date represents when location factor becan to be collected in
            -- source data.
            iff(
                locality_group = 1,
                least('2020-03-24', first_file_date),
                first_file_date
            ) as valid_from,
            max(coalesce(next_entry,{{ var("tomorrow") }})) over (
                partition by country, state_or_province, locality_group
            )::date as valid_to,
            boolor_agg(is_current) over (
                partition by country, state_or_province, locality_group
            ) as is_current_file
        from grouping

    )

select *
from final
