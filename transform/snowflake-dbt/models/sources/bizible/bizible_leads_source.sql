with
    source as (

        select

            id as lead_id,
            modified_date as modified_date,
            created_date as created_date,
            email as email,
            web_site as web_site,
            company as company,
            lead_source as lead_source,
            is_converted as is_converted,
            converted_opportunity_id as converted_opportunity_id,
            converted_date as converted_date,
            converted_contact_id as converted_contact_id,
            accountid as accountid,
            bizible_stage as bizible_stage,
            bizible_stage_previous as bizible_stage_previous,
            odds_of_conversion as odds_of_conversion,
            lead_score_model as lead_score_model,
            lead_score_results as lead_score_results,
            bizible_cookie_id as bizible_cookie_id,
            is_deleted as is_deleted,
            is_duplicate as is_duplicate,
            source_system as source_system,
            other_system_id as other_system_id,
            custom_properties as custom_properties,
            row_key as row_key,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_leads") }}

    )

select *
from source
