{{
    config(
        {
            "materialized": "table",
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("salesforce", "opportunity_split") }}),
    renamed as (

        select
            id::varchar as opportunity_split_id,
            opportunityid::varchar as opportunity_id,
            createdbyid::varchar as created_by_id,
            createddate::timestamp as created_date,
            lastmodifiedbyid::varchar as last_modified_by_id,
            lastmodifieddate::timestamp as last_modified_date,
            employee_number__c::varchar as employee_number,
            isdeleted::boolean as is_deleted,
            opp_owner_different__c::boolean as is_opportunity_owner_different,
            split::number as split,
            split_id__c::varchar as split_id,
            split_owner_role__c::varchar as split_owner_role,
            splitamount::number as split_amount,
            splitnote::varchar as split_note,
            splitownerid::varchar as split_owner_id,
            splitpercentage::number as split_percentage,
            splittypeid::varchar as split_type_id,
            team__c::varchar as team,
            user_role__c::varchar as user_role,
            systemmodstamp::timestamp as system_mod_timestamp
        from source
    )

select *
from renamed
