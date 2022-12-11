{{ config({"materialized": "incremental", "unique_key": "source_primary_key"}) }}

with
    source as (

        select *
        from {{ ref("gcp_billing_export_source") }}
        {% if is_incremental() %}

        where uploaded_at >= (select max(uploaded_at) from {{ this }})

        {% endif %}

    ),
    credits as (

        select
            source_primary_key as source_primary_key,
            sum(ifnull(credit_amount, 0)) as total_credits
        from {{ ref("gcp_billing_export_credits") }}
        group by 1

    ),
    renamed as (

        select
            source.primary_key as source_primary_key,
            source.billing_account_id as billing_account_id,
            source.service_id as service_id,
            source.service_description as service_description,
            source.sku_id as sku_id,
            source.sku_description as sku_description,
            source.invoice_month as invoice_month,
            source.usage_start_time as usage_start_time,
            source.usage_end_time as usage_end_time,
            source.project_id as project_id,
            source.project_name as project_name,
            source.project_labels as project_labels,
            source.folder_id as folder_id,
            source.resource_location as resource_location,
            source.resource_zone as resource_zone,
            source.resource_region as resource_region,
            source.resource_country as resource_country,
            source.labels as resource_labels,
            source.system_labels as system_labels,
            source.cost as cost_before_credits,
            credits.total_credits as total_credits,
            source.cost + ifnull(credits.total_credits, 0) as total_cost,
            source.usage_amount as usage_amount,
            source.usage_unit as usage_unit,
            source.usage_amount_in_pricing_units as usage_amount_in_pricing_units,
            source.pricing_unit as pricing_unit,
            source.currency as currency,
            source.currency_conversion_rate as currency_conversion_rate,
            source.cost_type as cost_type,
            source.credits as credits,
            source.export_time as export_time,
            source.uploaded_at as uploaded_at
        from source
        left join credits on source.primary_key = credits.source_primary_key

    )

select *
from renamed
