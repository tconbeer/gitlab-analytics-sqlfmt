with
    source as (

        select
            id as email_to_visitor_id,
            email as email,
            visitor_id as visitor_id,
            modified_date as modified_date,
            created_date as created_date,
            is_ignore as is_ignore,
            is_deleted as is_deleted,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_email_to_visitor_ids") }}

    )

select *
from source
