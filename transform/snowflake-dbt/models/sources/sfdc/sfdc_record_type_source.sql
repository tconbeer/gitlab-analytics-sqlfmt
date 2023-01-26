with
    source as (select * from {{ source("salesforce", "record_type") }}),
    renamed as (

        select
            id as record_type_id,
            developername as record_type_name,
            -- keys
            businessprocessid as business_process_id,
            -- info
            name as record_type_label,
            description as record_type_description,
            sobjecttype as record_type_modifying_object_type

        from source

    )

select *
from renamed
