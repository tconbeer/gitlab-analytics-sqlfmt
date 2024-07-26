with
    source as (select * from {{ source("marketo", "activity_merge_leads") }}),
    renamed as (

        select

            id::number as marketo_activity_merge_leads_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            merge_ids::text as merge_ids,
            merged_in_sales::boolean as merged_in_sales,
            merge_source::text as merge_source,
            master_updated::boolean as master_updated

        from source

    )

select *
from renamed
