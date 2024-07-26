with
    source as (select * from {{ source("zendesk", "organizations") }}),

    renamed as (

        select

            -- ids
            id as organization_id,
            organization_fields__salesforce_id::varchar as sfdc_account_id,

            -- fields
            name as organization_name,
            tags as organization_tags,
            organization_fields__aar::number as arr,
            organization_fields__market_segment::varchar as organization_market_segment,

            -- dates
            created_at,
            updated_at

        from source

    )

select *
from renamed
