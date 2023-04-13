with
    source as (

        select

            id as opportunity_id,
            modified_date as modified_date,
            created_date as created_date,
            account_id as account_id,
            name as name,
            is_won as is_won,
            is_closed as is_closed,
            close_date as close_date,
            bizible_custom_model_date as bizible_custom_model_date,
            amount as amount,
            converted_from_lead_id as converted_from_lead_id,
            converted_from_lead_email as converted_from_lead_email,
            primary_contact_id as primary_contact_id,
            primary_contact_email as primary_contact_email,
            odds_of_conversion as odds_of_conversion,
            bizible_stage as bizible_stage,
            bizible_stage_previous as bizible_stage_previous,
            is_deleted as is_deleted,
            custom_properties as custom_properties,
            currency_iso_code as currency_iso_code,
            row_key as row_key,
            currency_id as currency_id,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_opportunities") }}

    )

select *
from source
