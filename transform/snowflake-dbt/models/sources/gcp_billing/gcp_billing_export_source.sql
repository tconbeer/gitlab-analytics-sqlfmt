{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    source as (

        select *
        from {{ source("gcp_billing", "gcp_billing_export_combined") }}
        {% if is_incremental() %}

        where uploaded_at >= (select max(uploaded_at) from {{ this }})

        {% endif %}

    ),
    flattened as (

        select
            flatten_export.value['billing_account_id']::varchar as billing_account_id,
            flatten_export.value['cost']::float as cost,
            flatten_export.value['cost_type']::varchar as cost_type,
            flatten_export.value['credits']::variant as credits,
            flatten_export.value['currency']::varchar as currency,
            flatten_export.value[
                'currency_conversion_rate'
            ]::float as currency_conversion_rate,
            flatten_export.value['export_time']::timestamp as export_time,
            to_date(
                flatten_export.value['invoice'] ['month']::string, 'YYYYMM'
            ) as invoice_month,
            flatten_export.value['labels']::variant as labels,
            flatten_export.value['location'] ['country']::varchar as resource_country,
            flatten_export.value['location'] ['location']::varchar as resource_location,
            flatten_export.value['location'] ['region']::varchar as resource_region,
            flatten_export.value['location'] ['zone']::varchar as resource_zone,
            flatten_export.value['project'] ['ancestry_numbers']::varchar as folder_id,
            flatten_export.value['project'] ['id']::varchar as project_id,
            flatten_export.value['project'] ['labels']::variant as project_labels,
            flatten_export.value['project'] ['name']::varchar as project_name,
            flatten_export.value['service'] ['id']::varchar as service_id,
            flatten_export.value['service'] [
                'description'
            ]::varchar as service_description,
            flatten_export.value['sku'] ['id']::varchar as sku_id,
            flatten_export.value['sku'] ['description']::varchar as sku_description,
            flatten_export.value['system_labels']::variant as system_labels,
            flatten_export.value['usage'] ['pricing_unit']::varchar as pricing_unit,
            flatten_export.value['usage'] ['amount']::float as usage_amount,
            flatten_export.value['usage'] [
                'amount_in_pricing_units'
            ]::float as usage_amount_in_pricing_units,
            flatten_export.value['usage'] ['unit']::varchar as usage_unit,
            flatten_export.value['usage_start_time']::timestamp as usage_start_time,
            flatten_export.value['usage_end_time']::timestamp as usage_end_time,
            source.uploaded_at as uploaded_at,
            {{
                dbt_utils.surrogate_key(
                    [
                        "billing_account_id",
                        "cost",
                        "cost_type",
                        "credits",
                        "currency",
                        "currency_conversion_rate",
                        "export_time",
                        "invoice_month",
                        "labels",
                        "resource_country",
                        "resource_location",
                        "resource_region",
                        "resource_zone",
                        "folder_id",
                        "project_id",
                        "project_labels",
                        "project_name",
                        "service_id",
                        "service_description",
                        "sku_id",
                        "sku_description",
                        "system_labels",
                        "pricing_unit",
                        "usage_amount",
                        "usage_amount_in_pricing_units",
                        "usage_unit",
                        "usage_start_time",
                        "usage_end_time",
                    ]
                )
            }} as primary_key

        from source, table(flatten(source.jsontext)) flatten_export

    ),
    grouped as (
        select
            primary_key,
            billing_account_id,
            cost_type,
            credits,
            currency,
            currency_conversion_rate,
            export_time,
            invoice_month,
            labels,
            resource_country,
            resource_location,
            resource_region,
            resource_zone,
            folder_id,
            project_id,
            project_labels,
            project_name,
            service_id,
            service_description,
            sku_id,
            sku_description,
            system_labels,
            pricing_unit,
            usage_unit,
            usage_start_time,
            usage_end_time,
            -- rows can have identical primary keys, but differerent uploaded_at times
            -- so this allows these to be grouped together still
            max(uploaded_at) as uploaded_at,
            sum(cost) as cost,
            sum(usage_amount) as usage_amount,
            sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units
        from flattened {{ dbt_utils.group_by(n=26) }}
    -- UALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY uploaded_at DESC) =
    -- 1
    )

select *
from grouped
