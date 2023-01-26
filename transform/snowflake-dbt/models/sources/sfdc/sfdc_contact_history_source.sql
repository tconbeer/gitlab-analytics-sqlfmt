with
    base as (select * from {{ source("salesforce", "contact_history") }}),
    renamed as (

        select
            contactid as contact_id,
            id as contact_history_id,
            createddate as field_modified_at,
            lower(field) as contact_field,
            newvalue__fl as new_value_float,
            newvalue__de as new_value_decimal,
            oldvalue__fl as old_value_float,
            oldvalue__de as old_value_decimal,
            isdeleted as is_deleted,
            createdbyid as created_by_id
        from base

    )

select *
from renamed
