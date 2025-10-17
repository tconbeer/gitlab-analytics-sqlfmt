with
    source as (

        select
            id as account_id,
            created_date as created_date,
            modified_date as modified_date,
            name as name,
            web_site as web_site,
            engagement_rating as engagement_rating,
            engagement_score as engagement_score,
            domain as domain,
            is_deleted as is_deleted,
            custom_properties as custom_properties,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date
        from {{ source("bizible", "biz_accounts") }}

    )

select *
from source
