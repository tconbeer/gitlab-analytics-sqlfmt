with
    source as (select * from {{ source("sheetload", "planned_values") }}),
    renamed as (

        select
            unique_key::number as primary_key,
            plan_month::date as plan_month,
            planned_new_pipe::number as planned_new_pipe,
            planned_total_iacv::number as planned_total_iacv,
            planned_tcv_minus_gross_opex::number as planned_tcv_minus_gross_opex,
            planned_total_arr::number as planned_total_arr,
            sales_efficiency_plan::float as sales_efficiency_plan,
            magic_number_plan::float as magic_number_plan,
            tcv_plan::number as tcv_plan,
            acv_plan::number as acv_plan,
            planned_new_iacv::number as planned_new_iacv,
            planned_growth_iacv::number as planned_growth_iacv,
            iacv_divided_by_capcon_plan::float as iacv_divided_by_capcon_plan,
            planned_iacv_ent_apac::number as planned_iacv_ent_apac,
            planned_iacv_ent_emea::number as planned_iacv_ent_emea,
            planned_iacv_ent_pubsec::number as planned_iacv_ent_pubsec,
            planned_iacv_ent_us_west::number as planned_iacv_ent_us_west,
            planned_iacv_ent_us_east::number as planned_iacv_ent_us_east,
            planned_iacv_mm::number as planned_iacv_mm,
            planned_iacv_smb::number as planned_iacv_sbm,
            planned_pio_ent_apac::number as planned_pio_ent_apac,
            planned_pio_ent_emea::number as planned_pio_ent_emea,
            planned_pio_ent_pubsec::number as planned_pio_ent_pubsec,
            planned_pio_ent_us_west::number as planned_pio_ent_us_west,
            planned_pio_ent_us_east::number as planned_pio_ent_us_east,
            planned_pio_mm::number as planned_pio_mm,
            planned_pio_smb::number as planned_pio_sbm,
            planned_pio_amer::number as planned_pio_amer,
            planned_pio_emea::number as planned_pio_emea,
            planned_pio_apac::number as planned_pio_apac,
            planned_pio_pubsec::number as planned_pio_pubsec

        from source

    )

select *
from renamed
