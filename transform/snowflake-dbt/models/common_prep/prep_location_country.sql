with
    location_region as (select * from {{ ref("prep_location_region") }}),
    maxmind_countries_source as (

        select * from {{ ref("sheetload_maxmind_countries_source") }}

    ),
    zuora_country_geographic_region as (

        select * from {{ ref("zuora_country_geographic_region") }}

    ),
    joined as (

        select

            geoname_id as dim_location_country_id,
            country_name as country_name,
            upper(country_iso_code) as iso_2_country_code,
            upper(iso_alpha_3_code) as iso_3_country_code,
            continent_name,
            case
                when continent_name in ('Africa', 'Europe')
                then 'EMEA'
                when continent_name in ('North America')
                then 'AMER'
                when continent_name in ('South America')
                then 'LATAM'
                when continent_name in ('Oceania', 'Asia')
                then 'APAC'
                else 'Missing location_region_name'
            end as location_region_name_map,
            is_in_european_union

        from maxmind_countries_source
        left join
            zuora_country_geographic_region
            on upper(maxmind_countries_source.country_iso_code)
            = upper(zuora_country_geographic_region.iso_alpha_2_code)
        where country_iso_code is not null

    ),
    final as (

        select

            joined.dim_location_country_id,
            location_region.dim_location_region_id,
            joined.location_region_name_map,
            joined.country_name,
            joined.iso_2_country_code,
            joined.iso_3_country_code,
            joined.continent_name,
            joined.is_in_european_union

        from joined
        left join
            location_region
            on joined.location_region_name_map = location_region.location_region_name

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@mcooperDD",
            updated_by="@mcooperDD",
            created_date="2021-01-25",
            updated_date="2021-01-25",
        )
    }}
