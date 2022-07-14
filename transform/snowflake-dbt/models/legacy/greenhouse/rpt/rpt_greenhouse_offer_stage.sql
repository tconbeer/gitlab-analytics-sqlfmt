with
    recruiting_xf as (

        select *
        from {{ ref("greenhouse_recruiting_xf") }}
        where offer_id is not null and offer_status <> 'rejected'

    ),
    greenhouse_offer_custom_fields as (

        select
            offer_id,
            offer_custom_field,
            offer_custom_field_display_value as candidate_country
        from {{ ref("greenhouse_offer_custom_fields_source") }}
        where offer_custom_field = 'Candidate Country'

    ),
    zuora_regions as (select * from {{ ref("zuora_country_geographic_region") }}),
    bamboohr as (

        select
            greenhouse_candidate_id,
            iff(region = 'JAPAC', 'Asia Pacific', region) as region
        from {{ ref("bamboohr_id_employee_number_mapping") }}

    ),
    location_cleaned as (

        select
            offer_id,
            candidate_country,
            iff(
                lower(left(candidate_country, 12)) = 'united state',
                'North America',
                coalesce(
                    z1.geographic_region,
                    z2.geographic_region,
                    z3.geographic_region,
                    candidate_country
                )
            ) as geographic_region
        from greenhouse_offer_custom_fields
        left join
            zuora_regions z1
            on lower(z1.country_name_in_zuora) = lower(
                greenhouse_offer_custom_fields.candidate_country
            )
        left join
            zuora_regions z2
            on lower(z2.iso_alpha_2_code) = lower(
                greenhouse_offer_custom_fields.candidate_country
            )
        left join
            zuora_regions z3
            on lower(z3.iso_alpha_3_code) = lower(
                greenhouse_offer_custom_fields.candidate_country
            )

    ),
    data_set as (

        select
            recruiting_xf.offer_id,
            application_status,
            current_stage_name as stage_name,
            offer_status,
            offer_sent_date,
            offer_resolved_date,
            candidate_target_hire_date as start_date,
            candidate_country,
            geographic_region,
            bamboohr.region as bh_region,
            case
                when geographic_region in ('North America', 'South America')
                then geographic_region
                else coalesce(bh_region, geographic_region)
            end as region_final
        from recruiting_xf
        inner join
            bamboohr on recruiting_xf.candidate_id = bamboohr.greenhouse_candidate_id
        inner join
            location_cleaned on location_cleaned.offer_id = recruiting_xf.offer_id

    ),
    final as (

        select
            date_trunc(week, start_date) as start_week,
            region_final as geographic_region,
            count(offer_id) as candidates_estimated_to_start,
            sum(iff(offer_status = 'accepted', 1, 0)) as accepted_offers_to_start
        from data_set
        where
            geographic_region in (
                'North America', 'South America', 'EMEA', 'Asia Pacific', 'Americas'
            )
        group by 1, 2
        order by 1 desc

    )

select *
from final
