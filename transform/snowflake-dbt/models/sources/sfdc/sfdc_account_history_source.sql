with
    base as (select * from {{ source("salesforce", "account_history") }}),
    renamed as (

        select
            accountid as account_id,
            id as account_history_id,
            createddate as field_modified_at,
            lower(field) as account_field,
            newvalue__fl as new_value_float,
            newvalue__st as new_value_string,
            newvalue__bo as new_value_boolean,
            oldvalue__fl as old_value_float,
            oldvalue__st as old_value_string,
            oldvalue__bo as old_value_boolean,
            coalesce(
                newvalue__fl::varchar, newvalue__st::varchar, newvalue__bo::varchar
            ) as new_value,
            coalesce(
                oldvalue__fl::varchar, oldvalue__st::varchar, oldvalue__bo::varchar
            ) as old_value,
            isdeleted as is_deleted,
            createdbyid as created_by_id
        from base

    )

select *
from renamed
