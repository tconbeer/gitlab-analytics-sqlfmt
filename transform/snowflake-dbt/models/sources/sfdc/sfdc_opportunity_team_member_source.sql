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
    source as (select * from {{ source("salesforce", "opportunity_team_member") }}),
    renamed as (

        select
            id::varchar as opportunity_team_member_id,
            createdbyid::varchar as created_by_id,
            createddate::timestamp as created_date,
            employee_number__c::varchar as employee_number,
            isdeleted::boolean as is_deleted,
            lastmodifiedbyid::varchar as last_modified_by_id,
            lastmodifieddate::timestamp as last_modified_date,
            name::varchar as name,
            opportunityaccesslevel::varchar as opportunity_access_level,
            opportunityid::varchar as opportunity_id,
            photourl::varchar as photo_url,
            title::varchar as title,
            teammemberrole::varchar as team_member_role,
            userid::varchar as user_id,
            systemmodstamp::timestamp as system_mod_timestamp
        from source
    )

select *
from renamed
