with
    source as (

        select *
        from {{ source("sheetload", "sales_funnel_partner_alliance_targets_matrix") }}

    ),
    renamed as (

        select
            kpi_name::varchar as kpi_name,
            month::varchar as month,
            sales_qualified_source::varchar as sales_qualified_source,
            iff(
                sales_qualified_source::varchar = 'Channel Generated',
                'Partner Sourced',
                'Co-sell'
            ) as sqs_bucket_engagement,
            alliance_partner::varchar as alliance_partner,
            order_type::varchar as order_type,
            area::varchar as area, replace (
                allocated_target, ',', ''
            )::float as allocated_target,
            to_timestamp(to_numeric("_UPDATED_AT"))::timestamp as last_updated_at
        from source

    )

select *
from renamed
