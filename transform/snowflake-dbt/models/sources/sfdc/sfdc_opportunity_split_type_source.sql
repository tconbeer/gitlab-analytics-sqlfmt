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
    source as (select * from {{ source("salesforce", "opportunity_split_type") }}),
    renamed as (

        select

            id::varchar as opportunity_split_type_id,
            createdbyid::varchar as created_by_id,
            createddate::timestamp as created_date,
            lastmodifiedbyid::varchar as last_modified_by_id,
            lastmodifieddate::timestamp as last_modified_date,
            developername::varchar as developer_name,
            description::varchar as description,
            language::varchar as language,
            masterlabel::varchar as master_label,
            splitdatastatus::varchar as split_data_status,
            splitentity::varchar as split_entity,
            splitfield::varchar as split_field,
            isactive::boolean as is_active,
            istotalvalidated::boolean as is_total_validated,
            isdeleted::boolean as is_deleted,
            systemmodstamp::timestamp as system_mod_timestamp

        from source
    )

select *
from renamed
