with
    source as (

        select
            id as account_to_email_id,
            account_id as account_id,
            email as email,
            modified_date as modified_date,
            created_date as created_date,
            is_deleted as is_deleted,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_account_to_emails") }}

    )

select *
from source
