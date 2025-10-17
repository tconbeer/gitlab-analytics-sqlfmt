{{ config(tags=["mnpi"]) }}

with
    base as (select * from {{ source("salesforce", "opportunity_field_history") }}),
    renamed as (

        select
            opportunityid as opportunity_id,
            id as field_history_id,
            createddate as field_modified_at,
            lower(field) as opportunity_field,
            newvalue__fl as new_value_float,
            newvalue__st as new_value_string,
            newvalue__bo as new_value_boolean,
            newvalue__de as new_value_decimal,
            oldvalue__fl as old_value_float,
            oldvalue__st as old_value_string,
            oldvalue__bo as old_value_boolean,
            oldvalue__de as old_value_decimal,
            coalesce(
                newvalue__fl::varchar,
                newvalue__st::varchar,
                newvalue__bo::varchar,
                newvalue__de::varchar
            ) as new_value,
            coalesce(
                oldvalue__fl::varchar,
                oldvalue__st::varchar,
                oldvalue__bo::varchar,
                oldvalue__de::varchar
            ) as old_value,
            isdeleted as is_deleted,
            createdbyid as created_by_id
        from base

    )

select *
from renamed
