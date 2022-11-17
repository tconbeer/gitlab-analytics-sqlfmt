with
    source as (

        select *
        from {{ source("marketo", "activity_fill_out_linkedin_lead_gen_form") }}

    ),
    renamed as (

        select

            id::number as marketo_activity_fill_out_linkedin_lead_gen_form_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            lead_gen_campaign_name::text as lead_gen_campaign_name,
            lead_gen_creative_id::number as lead_gen_creative_id,
            lead_gen_account_name::text as lead_gen_account_name

        from source

    )

select *
from renamed
