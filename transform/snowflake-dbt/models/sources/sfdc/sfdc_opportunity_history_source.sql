{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("salesforce", "opportunity_history") }}),
    renamed as (
        select
            opportunityid as opportunity_id,
            id as opportunity_history_id,

            createddate as field_modified_at,
            createdbyid as created_by_id,
            createddate as created_date,
            closedate as close_date,
            forecastcategory as forecast_category,
            probability as probability,

            isdeleted as is_deleted,
            amount as amount,

            expectedrevenue as expected_revenue,
            stagename as stage_name,
            systemmodstamp as systemmodstamp
        from source

    )

select *
from renamed
