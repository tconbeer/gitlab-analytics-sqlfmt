{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("salesforce", "opportunity_stage") }}),
    renamed as (

        select
            id as sfdc_id,
            masterlabel as primary_label,
            defaultprobability as default_probability,
            forecastcategoryname as forecast_category_name,
            isactive as is_active,
            isclosed as is_closed,
            iswon as is_won
        from source

    )

select *
from renamed
