{{
    simple_cte(
        [
            ("bamboohr_locality", "bamboohr_id_employee_number_mapping_source"),
            ("locality", "dim_locality"),
        ]
    )
}},
final as (

    select
        bamboohr_locality.employee_number,
        bamboohr_locality.employee_id,
        bamboohr_locality.uploaded_at::date as updated_at,
        bamboohr_locality.locality as bamboo_locality,
        locality.location_factor
    from bamboohr_locality
    left join
        locality
        on lower(bamboohr_locality.locality) = lower(locality.locality)
        and date_trunc('day', bamboohr_locality.uploaded_at) >= locality.valid_from
        and date_trunc('day', bamboohr_locality.uploaded_at) < locality.valid_to
    where
        bamboohr_locality.locality is not null
        -- 1st time we started capturing locality
        and bamboohr_locality.uploaded_at >= '2020-03-24'

)

select *
from final
