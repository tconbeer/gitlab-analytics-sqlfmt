with
    source as (select * from {{ source("sheetload", "sdr_adaptive_data") }}),
    renamed as (

        select
            current_month::date as current_month,
            name::varchar as name,
            start_month::date as start_month,
            add_internal_accounted::varchar as add_internal_accounted,
            hiring_manager::varchar as hiring_manager,
            role::varchar as role,
            region::varchar as region,
            segment::varchar as segment,
            ghpid::number as ghpid,
            employment::varchar as employment,
            status::varchar as status,
            weighting::number as weighting,
            months_tenure::number as months_tenure,
            first_2_months::number as first_2_months,
            ramping::varchar as ramping,
            ramp_cutoff_date::date as ramp_cutoff_date,
            start_date_assumption::date as start_date_assumption
        from source
    )

select *
from renamed
