{{
    simple_cte(
        [
            ("geozones", "prep_geozone"),
            ("location_factor", "prep_location_factor"),
            ("director_factors", "director_location_factor_seed_source"),
        ]
    )
}},
geozone_titles as (

    select
        iff(geozone_country = 'United States', geozone_country, null) as country,
        geozone_locality,
        geozone_factor,
        min(valid_from) as valid_from,
        max(valid_to) as valid_to,
        booland_agg(is_current_file) as is_current_file
    from geozones
    group by 1, 2, 3

),
geozone_countries as (

    select
        geozone_country,
        country_locality,
        geozone_factor,
        min(valid_from) as valid_from,
        max(valid_to) as valid_to,
        booland_agg(is_current_file) as is_current_file
    from geozones
    group by 1, 2, 3

),
null_entry as (

    select *
    from
        (
            values(
                'unknown',
                'unknown',
                -1,
                {{ var("infinity_past") }},
                {{ var("infinity_future") }},
                false
            )
        ) as null_entry(country, locality, factor, valid_from, valid_to)

),
join_spine as (

    select distinct location_factor_country as country, valid_from
    from location_factor
    UNION
    select distinct geozone_country as country, valid_from
    from geozones

),
combined as (

    select distinct
        join_spine.country,
        coalesce(
            location_factor.locality, geozone_countries.country_locality
        ) as locality,
        coalesce(
            location_factor.location_factor, geozone_countries.geozone_factor
        ) as location_factor,
        coalesce(
            location_factor.valid_from, geozone_countries.valid_from
        ) as valid_from,
        coalesce(location_factor.valid_to, geozone_countries.valid_to) as valid_to,
        coalesce(
            location_factor.is_current_file, geozone_countries.is_current_file
        ) as is_current_source_file
    from join_spine
    left join
        geozone_countries
        on geozone_countries.geozone_country = join_spine.country
        and join_spine.valid_from >= geozone_countries.valid_from
        and join_spine.valid_from < geozone_countries.valid_to
    left join
        location_factor
        on location_factor.location_factor_country = join_spine.country
        and join_spine.valid_from >= location_factor.valid_from
        and join_spine.valid_from < location_factor.valid_to

    UNION

    select *
    from geozone_titles

    UNION

    select *
    from director_factors

    UNION

    select *
    from null_entry

),
final as (

    select
        {{ dbt_utils.surrogate_key(["lower(locality)"]) }} as dim_locality_id,
        locality,
        location_factor,
        country as locality_country,
        valid_from,
        valid_to,
        is_current_source_file
    from combined

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@pempey",
        updated_by="@pempey",
        created_date="2021-12-21",
        updated_date="2021-12-21",
    )
}}
