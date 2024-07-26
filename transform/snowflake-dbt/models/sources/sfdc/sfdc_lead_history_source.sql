with
    base as (select * from {{ source("salesforce", "lead_history") }}),
    renamed as (

        select
            id as lead_history_id,
            leadid as lead_id,
            createddate as field_modified_at,
            lower(field) as lead_field,
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
