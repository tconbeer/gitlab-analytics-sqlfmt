with
    source as (select * from {{ source("salesforce", "bizible_touchpoint") }}),
    renamed as (

        select
            id as touchpoint_id,
            bizible2__bizible_person__c as bizible_person_id,

            -- sfdc object lookups
            bizible2__sf_campaign__c as campaign_id,
            bizible2__contact__c as bizible_contact,
            bizible2__account__c as bizible_account,

            -- attribution counts
            bizible2__count_first_touch__c as bizible_count_first_touch,
            bizible2__count_lead_creation_touch__c as bizible_count_lead_creation_touch,
            bizible2__count_u_shaped__c as bizible_count_u_shaped,

            -- touchpoint info
            bizible2__touchpoint_date__c as bizible_touchpoint_date,
            bizible2__touchpoint_position__c as bizible_touchpoint_position,
            bizible2__touchpoint_source__c as bizible_touchpoint_source,
            source_type__c as bizible_touchpoint_source_type,
            bizible2__touchpoint_type__c as bizible_touchpoint_type,
            bizible2__ad_campaign_name__c as bizible_ad_campaign_name,
            bizible2__ad_content__c as bizible_ad_content,
            bizible2__ad_group_name__c as bizible_ad_group_name,
            bizible2__form_url__c as bizible_form_url,
            bizible2__form_url_raw__c as bizible_form_url_raw,
            bizible2__landing_page__c as bizible_landing_page,
            bizible2__landing_page_raw__c as bizible_landing_page_raw,
            bizible2__marketing_channel__c as bizible_marketing_channel,
            bizible2__marketing_channel_path__c as bizible_marketing_channel_path,
            bizible2__medium__c as bizible_medium,
            bizible2__referrer_page__c as bizible_referrer_page,
            bizible2__referrer_page_raw__c as bizible_referrer_page_raw,
            bizible2__sf_campaign__c as bizible_salesforce_campaign,
            utm_budget__c as utm_budget,
            utm_offersubtype__c as utm_offersubtype,
            utm_offertype__c as utm_offertype,
            utm_targetregion__c as utm_targetregion,
            utm_targetsubregion__c as utm_targetsubregion,
            utm_targetterritory__c as utm_targetterritory,
            utm_usecase__c as utm_usecase,
            case
                when
                    split_part(
                        split_part(bizible_form_url_raw, 'utm_content=', 2), '&', 1
                    )
                    is null
                then
                    split_part(
                        split_part(bizible_landing_page_raw, 'utm_content=', 2), '&', 1
                    )
                else
                    split_part(
                        split_part(bizible_form_url_raw, 'utm_content=', 2), '&', 1
                    )
            end as utm_content,

            isdeleted as is_deleted


        from source
    )

select *
from renamed
